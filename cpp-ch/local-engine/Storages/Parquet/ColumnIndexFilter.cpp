/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ColumnIndexFilter.h"

#if USE_PARQUET
#include <ranges>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/Parquet/ParquetConverter.h>
#include <fmt/ranges.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <Common/logger_useful.h>

namespace local_engine
{
struct BoundaryOrder;
struct UNORDERED;
struct ASCENDING;
struct DESCENDING;

template <typename DType>
class TypedComparator;

struct NoneNullPageIndexsBuilder
{
    explicit NoneNullPageIndexsBuilder(const std::vector<Int32> & non_null_page_indices) : non_null_page_indices_(non_null_page_indices) { }
    const std::vector<Int32> & non_null_page_indices_;

    PageIndexs all() const { return non_null_page_indices_; }
    PageIndexs range(const Int32 from, const Int32 to) const
    {
        const auto begin = non_null_page_indices_.begin() + from;
        const auto end = non_null_page_indices_.begin() + to;
        return PageIndexs{begin, end};
    }

    template <typename Predict>
    PageIndexs filter(Int32 from, const Int32 to, Predict predict) const
    {
        PageIndexs pages;
        for (; from != to; ++from)
            if (predict(from))
                pages.emplace_back(non_null_page_indices_[from]);
        return pages;
    }

    template <typename Predict>
    PageIndexs filter(Predict predict) const
    {
        return filter(0, non_null_page_indices_.size(), predict);
    }
};

class EmptyColumnIndex final : public ColumnIndex
{
public:
    const parquet::OffsetIndex & offsetIndex() const override { return *offset_index_; }
    bool hasParquetColumnIndex() const override { return false; }

    PageIndexs notEq(const DB::Field &) const override { abort(); }
    PageIndexs eq(const DB::Field &) const override { abort(); }
    PageIndexs gt(const DB::Field &) const override { abort(); }
    PageIndexs gtEg(const DB::Field &) const override { abort(); }
    PageIndexs lt(const DB::Field &) const override { abort(); }
    PageIndexs ltEg(const DB::Field &) const override { abort(); }
    PageIndexs in(const DB::ColumnPtr &) const override { abort(); }

    explicit EmptyColumnIndex(const std::shared_ptr<parquet::OffsetIndex> & offset_index) : offset_index_(offset_index) { }

private:
    std::shared_ptr<parquet::OffsetIndex> offset_index_;
};

template <typename T, typename Base>
concept Derived = std::is_base_of_v<Base, T>;

template <typename DType, Derived<BoundaryOrder> ORDER>
class TypedColumnIndexImpl final : public TypedColumnIndex<DType>
{
    const parquet::ColumnDescriptor * descr_;
    std::shared_ptr<parquet::TypedColumnIndex<DType>> column_index_;
    std::shared_ptr<parquet::OffsetIndex> offset_index_;
    std::shared_ptr<parquet::TypedComparator<DType>> comparator_;

public:
    using T = typename DType::c_type;

    TypedColumnIndexImpl(
        const parquet::ColumnDescriptor * descr,
        const std::shared_ptr<parquet::TypedColumnIndex<DType>> & column_index,
        const std::shared_ptr<parquet::OffsetIndex> & offset_index)
        : descr_(descr), column_index_(column_index), offset_index_(offset_index), comparator_(parquet::MakeComparator<DType>(descr))
    {
    }
    PageIndexs notEq(const DB::Field & value) const override;
    PageIndexs eq(const DB::Field & value) const override;
    PageIndexs gt(const DB::Field & value) const override;
    PageIndexs gtEg(const DB::Field & value) const override;
    PageIndexs lt(const DB::Field & value) const override;
    PageIndexs ltEg(const DB::Field & value) const override;
    PageIndexs in(const DB::ColumnPtr & column) const override;

    const parquet::OffsetIndex & offsetIndex() const override { return *offset_index_; }
    bool hasParquetColumnIndex() const override { return true; }
};

/*
 * A class containing the value to be compared to the min/max values. This way we only need to do the deboxing once
 * per predicate execution instead for every comparison.
 */
template <typename DType>
class TypedComparator
{
    using T = typename DType::c_type;
    const T & value_;
    const std::vector<T> & min_;
    const std::vector<T> & max_;
    const std::vector<Int32> & non_null_page_indices_;
    parquet::TypedComparator<DType> & comparator_;
    friend UNORDERED;
    friend ASCENDING;
    friend DESCENDING;

public:
    TypedComparator(const T & value, const parquet::TypedColumnIndex<DType> & index, parquet::TypedComparator<DType> & comparator)
        : value_(value)
        , min_(index.min_values())
        , max_(index.max_values())
        , non_null_page_indices_(index.non_null_page_indices())
        , comparator_(comparator)
    {
    }
    size_t size() const { return non_null_page_indices_.size(); }
    Int32 compareValueToMin(size_t non_null_page_index) const
    {
        const T & min = min_[non_null_page_indices_[non_null_page_index]];
        return comparator_.Compare(value_, min) ? -1 : comparator_.Compare(min, value_) ? 1 : 0;
    }
    Int32 compareValueToMax(size_t non_null_page_index) const
    {
        const T & max = max_[non_null_page_indices_[non_null_page_index]];
        return comparator_.Compare(value_, max) ? -1 : comparator_.Compare(max, value_) ? 1 : 0;
    }
};

struct Bounds
{
    const Int32 lower;
    const Int32 upper;

    Bounds(const Int32 l, const Int32 u) : lower(l), upper(u) { }
};

struct BoundaryOrder
{
    /// Avoid the possible overflow might happen in case of (left + right) / 2
    static Int32 floorMid(const Int32 lower, const Int32 upper) { return lower + (upper - lower) / 2; }

    /// Avoid the possible overflow might happen in case of (left + right + 1) / 2
    static Int32 ceilingMid(const Int32 lower, const Int32 upper) { return lower + (upper - lower + 1) / 2; }
};

struct UNORDERED : BoundaryOrder
{
    template <typename DType>
    static PageIndexs eq(const TypedComparator<DType> & comparator)
    {
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.filter(
            [&](const Int32 i) { return comparator.compareValueToMin(i) >= 0 && comparator.compareValueToMax(i) <= 0; });
    }

    template <typename DType>
    static PageIndexs gt(const TypedComparator<DType> & comparator)
    {
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.filter([&](const Int32 i)
                                                                                   { return comparator.compareValueToMax(i) < 0; });
    }

    template <typename DType>
    static PageIndexs gtEq(const TypedComparator<DType> & comparator)
    {
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.filter([&](const Int32 i)
                                                                                   { return comparator.compareValueToMax(i) <= 0; });
    }

    template <typename DType>
    static PageIndexs lt(const TypedComparator<DType> & comparator)
    {
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.filter([&](const Int32 i)
                                                                                   { return comparator.compareValueToMin(i) > 0; });
    }

    template <typename DType>
    static PageIndexs ltEq(const TypedComparator<DType> & comparator)
    {
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.filter([&](const Int32 i)
                                                                                   { return comparator.compareValueToMin(i) >= 0; });
    }

    template <typename DType>
    static PageIndexs notEq(const TypedComparator<DType> & comparator)
    {
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.filter(
            [&](const Int32 i) { return comparator.compareValueToMin(i) != 0 || comparator.compareValueToMax(i) != 0; });
    }
};

struct ASCENDING : BoundaryOrder
{
    template <typename DType>
    static std::optional<Bounds> findBounds(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        Int32 lowerLeft = 0;
        Int32 upperLeft = 0;
        Int32 lowerRight = length - 1;
        Int32 upperRight = length - 1;

        do
        {
            if (lowerLeft > lowerRight)
                return std::nullopt;

            const Int32 i = floorMid(lowerLeft, lowerRight);
            if (comparator.compareValueToMin(i) < 0)
                lowerRight = upperRight = i - 1;
            else if (comparator.compareValueToMax(i) > 0)
                lowerLeft = upperLeft = i + 1;
            else
                lowerRight = upperLeft = i;
        } while (lowerLeft != lowerRight);

        do
        {
            if (upperLeft > upperRight)
                return std::nullopt;

            const Int32 i = ceilingMid(upperLeft, upperRight);
            if (comparator.compareValueToMin(i) < 0)
                upperRight = i - 1;
            else if (comparator.compareValueToMax(i) > 0)
                upperLeft = i + 1;
            else
                upperLeft = i;
        } while (upperLeft != upperRight);

        return Bounds(lowerLeft, upperRight);
    }

    template <typename DType>
    static PageIndexs eq(const TypedComparator<DType> & comparator)
    {
        const NoneNullPageIndexsBuilder builder(comparator.non_null_page_indices_);
        return findBounds(comparator)
            .transform([&](const Bounds & b) { return builder.range(b.lower, b.upper + 1); })
            .value_or(PageIndexs{});
    }

    template <typename DType>
    static PageIndexs gt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            Int32 i = floorMid(left, right);
            if (comparator.compareValueToMax(i) >= 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(right, length);
    }

    template <typename DType>
    static PageIndexs gtEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            Int32 i = floorMid(left, right);
            if (comparator.compareValueToMax(i) > 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(right, length);
    }

    template <typename DType>
    static PageIndexs lt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMin(i) <= 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);

        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(0, left + 1);
    }

    template <typename DType>
    static PageIndexs ltEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMin(i) < 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(0, left + 1);
    }

    template <typename DType>
    static PageIndexs notEq(const TypedComparator<DType> & comparator)
    {
        NoneNullPageIndexsBuilder builder{comparator.non_null_page_indices_};
        return findBounds(comparator)
            .transform(
                [&](const Bounds & b)
                {
                    return builder.filter(
                        [&](const Int32 i)
                        {
                            return i < b.lower || i > b.upper || comparator.compareValueToMin(i) != 0
                                || comparator.compareValueToMax(i) != 0;
                        });
                })
            .value_or(builder.all());
    }
};

struct DESCENDING : BoundaryOrder
{
    template <typename DType>
    static std::optional<Bounds> findBounds(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        Int32 lowerLeft = 0;
        Int32 upperLeft = 0;
        Int32 lowerRight = length - 1;
        Int32 upperRight = length - 1;

        do
        {
            if (lowerLeft > lowerRight)
                return std::nullopt;
            Int32 i = floorMid(lowerLeft, lowerRight);
            if (comparator.compareValueToMax(i) > 0)
                lowerRight = upperRight = i - 1;
            else if (comparator.compareValueToMin(i) < 0)
                lowerLeft = upperLeft = i + 1;
            else
                lowerRight = upperLeft = i;
        } while (lowerLeft != lowerRight);

        do
        {
            if (upperLeft > upperRight)
                return std::nullopt;
            Int32 i = ceilingMid(upperLeft, upperRight);
            if (comparator.compareValueToMax(i) > 0)
                upperRight = i - 1;
            else if (comparator.compareValueToMin(i) < 0)
                upperLeft = i + 1;
            else
                upperLeft = i;
        } while (upperLeft != upperRight);

        return Bounds(lowerLeft, upperRight);
    }

    template <typename DType>
    static PageIndexs eq(const TypedComparator<DType> & comparator)
    {
        const NoneNullPageIndexsBuilder builder{comparator.non_null_page_indices_};
        return findBounds(comparator)
            .transform([&](const Bounds & b) { return builder.range(b.lower, b.upper + 1); })
            .value_or(PageIndexs{});
    }
    template <typename DType>
    static PageIndexs gt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMax(i) >= 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(0, left + 1);
    }

    template <typename DType>
    static PageIndexs gtEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return {};
        }
        Int32 left = -1;
        Int32 right = length - 1;
        do
        {
            Int32 i = ceilingMid(left, right);
            if (comparator.compareValueToMax(i) > 0)
                right = i - 1;
            else
                left = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(0, left + 1);
    }

    template <typename DType>
    static PageIndexs lt(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return PageIndexs{};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            Int32 i = floorMid(left, right);
            if (comparator.compareValueToMin(i) <= 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(right, length);
    }

    template <typename DType>
    static PageIndexs ltEq(const TypedComparator<DType> & comparator)
    {
        const Int32 length = comparator.size();
        if (length == 0)
        {
            // No matching rows if the column index contains null pages only
            return PageIndexs{};
        }
        Int32 left = 0;
        Int32 right = length;
        do
        {
            Int32 i = floorMid(left, right);
            if (comparator.compareValueToMin(i) < 0)
                left = i + 1;
            else
                right = i;
        } while (left < right);
        return NoneNullPageIndexsBuilder{comparator.non_null_page_indices_}.range(right, length);
    }
    template <typename DType>
    static PageIndexs notEq(const TypedComparator<DType> & comparator)
    {
        const NoneNullPageIndexsBuilder builder{comparator.non_null_page_indices_};
        return findBounds(comparator)
            .transform(
                [&](const Bounds & b)
                {
                    return builder.filter(
                        [&](const Int32 i)
                        {
                            return i < b.lower || i > b.upper || comparator.compareValueToMin(i) != 0
                                || comparator.compareValueToMin(i) != 0;
                        });
                })
            .value_or(builder.all());
    }
};

/// TODO: bencnmark
template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::notEq(const DB::Field & value) const
{
    if (value.isNull())
    {
        return PageIndexsBuilder::filter(
            column_index_->null_pages().size(), [&](const Int32 i) { return !column_index_->null_pages()[i]; });
    }
    if (!column_index_->has_null_counts())
    {
        // Nulls match so if we don't have null related statistics we have to return all pages
        return {PageIndexsBuilder::ALL_PAGES};
    }

    // Merging value filtering with pages containing nulls
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index_, *comparator_};
    auto pages = ORDER::notEq(typed_comparator);
    const std::set<size_t> matchingIndexes(pages.begin(), pages.end());

    return PageIndexsBuilder::filter(
        column_index_->null_counts().size(),
        [&](const Int32 i) { return column_index_->null_counts()[i] > 0 || matchingIndexes.contains(i); });
}

template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::eq(const DB::Field & value) const
{
    if (value.isNull())
    {
        if (column_index_->has_null_counts())
        {
            return PageIndexsBuilder::filter(
                column_index_->null_counts().size(), [&](const Int32 i) { return column_index_->null_counts()[i] > 0; });
        }
        else
        {
            // Searching for nulls so if we don't have null related statistics we have to return all pages
            return {PageIndexsBuilder::ALL_PAGES};
        }
    }
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index_, *comparator_};
    return ORDER::eq(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::gt(const DB::Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index_, *comparator_};
    return ORDER::gt(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::gtEg(const DB::Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index_, *comparator_};
    return ORDER::gtEq(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::lt(const DB::Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index_, *comparator_};
    return ORDER::lt(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::ltEg(const DB::Field & value) const
{
    ToParquet<DType> to_parquet;
    auto real_value{to_parquet.as(value, *descr_)};
    TypedComparator<DType> typed_comparator{real_value, *column_index_, *comparator_};
    return ORDER::ltEq(typed_comparator);
}

template <typename DType, Derived<BoundaryOrder> ORDER>
PageIndexs TypedColumnIndexImpl<DType, ORDER>::in(const DB::ColumnPtr & column) const
{
    /// TDDO: handle null
    ///
    std::shared_ptr<ParquetConverter<DType>> converter = ParquetConverter<DType>::Make(column, *descr_);
    const auto * value = converter->getBatch(0, column->size());
    T min, max;
    std::tie(min, max) = comparator_->GetMinMax(value, column->size());

    TypedComparator<DType> typed_comparator_min{min, *column_index_, *comparator_};
    PageIndexs min_page_indexs = ORDER::gtEq(typed_comparator_min);
    std::ranges::sort(min_page_indexs);


    TypedComparator<DType> typed_comparator_max{max, *column_index_, *comparator_};
    PageIndexs max_page_indexs = ORDER::ltEq(typed_comparator_max);
    std::ranges::sort(max_page_indexs);

    std::set<size_t> matchingIndex;
    std::ranges::set_intersection(min_page_indexs, max_page_indexs, std::inserter(matchingIndex, matchingIndex.begin()));

    return PageIndexsBuilder::filter(column_index_->null_counts().size(), [&](const Int32 i) { return matchingIndex.contains(i); });
}

template <typename T, typename S>
std::unique_ptr<T> dynamic_pointer_cast(std::unique_ptr<S> && p) noexcept
{
    if (T * const converted = dynamic_cast<T *>(p.get()))
    {
        // cast succeeded; clear input
        p.release();
        return std::unique_ptr<T>{converted};
    }
    // cast failed; leave input untouched
    throw DB::Exception(
        DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to {}", demangle(typeid(S).name()), demangle(typeid(T).name()));
}

template <typename ORDER>
ColumnIndexPtr internalMakeColumnIndex(
    const parquet::ColumnDescriptor * descr,
    const std::shared_ptr<parquet::ColumnIndex> & column_index,
    const std::shared_ptr<parquet::OffsetIndex> & offset_index)
{
    auto physical_type = descr->physical_type();
    switch (physical_type)
    {
        case parquet::Type::BOOLEAN:
            return std::make_unique<TypedColumnIndexImpl<parquet::BooleanType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::BoolColumnIndex>(column_index), offset_index);
        case parquet::Type::INT32:
            return std::make_unique<TypedColumnIndexImpl<parquet::Int32Type, ORDER>>(
                descr, dynamic_pointer_cast<parquet::Int32ColumnIndex>(column_index), offset_index);
        case parquet::Type::INT64:
            return std::make_unique<TypedColumnIndexImpl<parquet::Int64Type, ORDER>>(
                descr, dynamic_pointer_cast<parquet::Int64ColumnIndex>(column_index), offset_index);
        case parquet::Type::INT96:
            break;
        case parquet::Type::FLOAT:
            return std::make_unique<TypedColumnIndexImpl<parquet::FloatType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::FloatColumnIndex>(column_index), offset_index);
        case parquet::Type::DOUBLE:
            return std::make_unique<TypedColumnIndexImpl<parquet::DoubleType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::DoubleColumnIndex>(column_index), offset_index);
        case parquet::Type::BYTE_ARRAY:
            return std::make_unique<TypedColumnIndexImpl<parquet::ByteArrayType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::ByteArrayColumnIndex>(column_index), offset_index);
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return std::make_unique<TypedColumnIndexImpl<parquet::FLBAType, ORDER>>(
                descr, dynamic_pointer_cast<parquet::FLBAColumnIndex>(column_index), offset_index);
        case parquet::Type::UNDEFINED:
            break;
    }
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported physical type {}", TypeToString(physical_type));
}

ColumnIndexPtr ColumnIndex::create(
    const parquet::ColumnDescriptor * descr,
    const std::shared_ptr<parquet::ColumnIndex> & column_index,
    const std::shared_ptr<parquet::OffsetIndex> & offset_index)
{
    if (!column_index)
        return std::make_unique<EmptyColumnIndex>(offset_index);

    const auto order = column_index->boundary_order();
    switch (order)
    {
        case parquet::BoundaryOrder::Unordered:
            return internalMakeColumnIndex<UNORDERED>(descr, column_index, offset_index);
        case parquet::BoundaryOrder::Ascending:
            return internalMakeColumnIndex<ASCENDING>(descr, column_index, offset_index);
        case parquet::BoundaryOrder::Descending:
            return internalMakeColumnIndex<DESCENDING>(descr, column_index, offset_index);
        default:
            break;
    }
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported UNDEFINED BoundaryOrder: {}", order);
}

///
const ColumnIndexFilter::AtomMap ColumnIndexFilter::atom_map{
    {"notEquals",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_NOT_EQUALS;
         out.value = value;
         return true;
     }},
    {"equals",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_EQUALS;
         out.value = value;
         return true;
     }},
    {"less",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_LESS;
         out.value = value;
         return true;
     }},
    {"greater",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_GREATER;
         out.value = value;
         return true;
     }},
    {"lessOrEquals",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_LESS_OR_EQUALS;
         out.value = value;
         return true;
     }},
    {"greaterOrEquals",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_GREATER_OR_EQUALS;
         out.value = value;
         return true;
     }},
    {"in",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field &)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_IN;
         return true;
     }},
    {"notIn",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field &)
     {
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_NOT_IN;
         return true;
     }},
    {"isNotNull",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field &)
     {
         /// Field's default constructor constructs a null value
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_NOT_EQUALS;
         return true;
     }},
    {"isNull",
     [](DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field &)
     {
         /// Field's default constructor constructs a null value
         out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_EQUALS;
         return true;
     }}};

ColumnIndexFilter::ColumnIndexFilter(const DB::ActionsDAG & filter_dag, DB::ContextPtr context)
{
    DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context);
    assert(inverted_dag.predicate != nullptr);

    DB::RPNBuilder<DB::GlutenParquetColumnIndexFilter::RPNElement> builder(
        inverted_dag.predicate,
        std::move(context),
        [&](const DB::RPNBuilderTreeNode & node, DB::GlutenParquetColumnIndexFilter::RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn_ = std::move(builder).extractRPN();
}

bool tryPrepareSetIndex(const DB::RPNBuilderFunctionTreeNode & func, DB::GlutenParquetColumnIndexFilter::RPNElement & out)
{
    const auto right_arg = func.getArgumentAt(1);
    const auto future_set = right_arg.tryGetPreparedSet();
    if (future_set->getTypes().size() != 1)
        return false;

    if (!future_set)
        return false;

    const auto prepared_set = future_set->buildOrderedSetInplace(right_arg.getTreeContext().getQueryContext());
    if (!prepared_set)
        return false;

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    const auto set_columns = prepared_set->getSetElements();
    assert(1 == set_columns.size());
    out.column = set_columns[0];
    out.columnName = func.getArgumentAt(0).getColumnName();
    return true;
}

bool ColumnIndexFilter::extractAtomFromTree(const DB::RPNBuilderTreeNode & node, DB::GlutenParquetColumnIndexFilter::RPNElement & out)
{
    DB::Field const_value;
    DB::DataTypePtr const_type;
    if (node.isFunction())
    {
        const auto func = node.toFunctionNode();
        const std::string func_name = func.getFunctionName();
        if (!atom_map.contains(func_name))
            return false;

        const size_t num_args = func.getArgumentsSize();
        if (num_args == 1)
        {
            out.columnName = func.getArgumentAt(0).getColumnName();
        }
        else if (num_args == 2)
        {
            if (DB::functionIsInOrGlobalInOperator(func_name))
            {
                if (!tryPrepareSetIndex(func, out))
                    return false;
            }
            else if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_FALSE;
                    return true;
                }
                out.columnName = func.getArgumentAt(0).getColumnName();
            }
            else
                return false;
        }
        else
            return false;

        if (out.columnName.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "`columnName` is empty. It is a bug.");

        const auto atom_it = atom_map.find(func_name);
        return atom_it->second(out, const_value);
    }
    else if (node.tryGetConstant(const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`
        if (const_value.getType() == DB::Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_TRUE : DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == DB::Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_TRUE : DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == DB::Field::Types::Float64)
        {
            out.function = const_value.safeGet<Float64>() != 0.0 ? DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_TRUE : DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
}

RowRanges ColumnIndexFilter::calculateRowRanges(const ColumnIndexStore & index_store, size_t rowgroup_count) const
{
    using LOGICAL_OP = RowRanges (*)(const RowRanges &, const RowRanges &);
    using OPERATOR = std::function<PageIndexs(const ColumnIndex &, const DB::GlutenParquetColumnIndexFilter::RPNElement &)>;
    std::vector<RowRanges> rpn_stack;

    auto CALL_LOGICAL_OP = [&rpn_stack](const LOGICAL_OP & op)
    {
        assert(rpn_stack.size() >= 2);
        const auto arg1 = rpn_stack.back();
        rpn_stack.pop_back();
        const auto arg2 = rpn_stack.back();
        rpn_stack.back() = op(arg1, arg2);
    };

    auto CALL_OPERATOR = [&rpn_stack, &index_store, rowgroup_count](const DB::GlutenParquetColumnIndexFilter::RPNElement & element, const OPERATOR & callback)
    {
        const auto it = index_store.find(element.columnName);
        if (it != index_store.end() && it->second->hasParquetColumnIndex())
        {
            const ColumnIndex & index = *it->second;
            const RowRangesBuilder rgbuilder(rowgroup_count, index.offsetIndex().page_locations());
            rpn_stack.emplace_back(rgbuilder.toRowRanges(callback(index, element)));
        }
        else
        {
            rpn_stack.emplace_back(RowRanges::createSingle(rowgroup_count));
            if (it == index_store.end())
            {
                LOG_WARNING(
                    &Poco::Logger::get("ColumnIndexFilter"),
                    "Column {} not found in ColumnIndexStore with Column name[{}], return all row ranges",
                    element.columnName,
                    fmt::join(index_store | std::ranges::views::transform([](const auto & kv) { return kv.first; }), ", "));
            }
        }
    };

    for (const auto & element : rpn_)
    {
        switch (element.function)
        {
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_EQUALS:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.eq(e.value); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_NOT_EQUALS:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.notEq(e.value); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_LESS:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.lt(e.value); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_GREATER:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.gt(e.value); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_LESS_OR_EQUALS:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.ltEg(e.value); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_GREATER_OR_EQUALS:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.gtEg(e.value); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_IN:
                CALL_OPERATOR(element, [](const ColumnIndex & index, const DB::GlutenParquetColumnIndexFilter::RPNElement & e) { return index.in(e.column); });
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_NOT_IN:
                rpn_stack.emplace_back(RowRanges::createSingle(rowgroup_count));
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_UNKNOWN:
                rpn_stack.emplace_back(RowRanges::createSingle(rowgroup_count));
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_NOT:
                assert(!rpn_stack.empty());
                rpn_stack.back() = RowRanges::createSingle(rowgroup_count);
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_AND:
                CALL_LOGICAL_OP(RowRanges::intersection);
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::FUNCTION_OR:
                CALL_LOGICAL_OP(RowRanges::unionRanges);
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_FALSE:
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_FALSE in ColumnIndexFilter::calculateRowRanges");
                break;
            case DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_TRUE:
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "DB::GlutenParquetColumnIndexFilter::RPNElement::ALWAYS_TRUE in ColumnIndexFilter::calculateRowRanges");
                break;
        }
    }

    if (rpn_stack.size() != 1)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in ColumnIndexFilter::calculateRowRanges");

    return rpn_stack[0];
}
}
#endif //USE_PARQUET