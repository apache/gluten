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
#pragma once
#include <config.h>

#if USE_PARQUET
#include <memory>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/Parquet/RowRanges.h>
#include <Storages/GlutenParquetColumnIndexFilter.h>
#include <parquet/page_index.h>

namespace DB
{
class RPNBuilderTreeNode;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
}

namespace local_engine
{
class ColumnIndexFilter;
class ColumnIndex;
using ColumnIndexPtr = std::unique_ptr<ColumnIndex>;
using PageIndexs = std::vector<Int32>;
using ColumnIndexStore = std::unordered_map<std::string, ColumnIndexPtr>;
using ColumnIndexFilterPtr = std::shared_ptr<ColumnIndexFilter>;

struct PageIndexsBuilder
{
    static constexpr Int32 ALL_PAGES = -1;
    template <typename Predict>
    static PageIndexs filter(const size_t size, Predict predict)
    {
        PageIndexs pages;
        for (Int32 from = 0; from != size; ++from)
            if (predict(from))
                pages.emplace_back(from);
        return pages;
    }
};

struct RowRangesBuilder
{
    const int64_t rg_count_; // the total number of rows in the row-group
    const std::vector<parquet::PageLocation> & page_locations_;

    RowRangesBuilder(int64_t row_group_row_count, const std::vector<parquet::PageLocation> & page_locations)
        : rg_count_(row_group_row_count), page_locations_(page_locations)
    {
    }

    /**
     * @param page_index
     *           the index of the page
     * @return the index of the first row in the page
     */
    size_t firstRowIndex(int page_index) const { return page_locations_[page_index].first_row_index; }

    /**
     * @param page_index
     *          the index of the page
     * @return the calculated index of the last row of the given page
     */
    size_t lastRowIndex(size_t page_index) const
    {
        const size_t nextPageIndex = page_index + 1;
        const size_t pageCount = std::ssize(page_locations_);

        const size_t lastRowIndex = (nextPageIndex >= pageCount ? rg_count_ : page_locations_[nextPageIndex].first_row_index) - 1;
        return lastRowIndex;
    }

    RowRanges all() const { return RowRanges::createSingle(rg_count_); }

    RowRanges toRowRanges(const PageIndexs & pages) const
    {
        if (pages.size() == 1 && pages[0] == PageIndexsBuilder::ALL_PAGES)
            return all();
        RowRanges row_ranges;
        std::ranges::for_each(pages, [&](size_t pageIndex) { row_ranges.add(Range{firstRowIndex(pageIndex), lastRowIndex(pageIndex)}); });
        return row_ranges;
    }
};

/**
 * Column index containing min/max and null count values for the pages in a column chunk. It also implements methods
 * to return the indexes of the matching pages.
 *
 * @see parquet::ColumnIndex
 */
class ColumnIndex
{
public:
    virtual ~ColumnIndex() = default;

    virtual const parquet::OffsetIndex & offsetIndex() const = 0;

    virtual bool hasParquetColumnIndex() const = 0;

    /// \brief Returns the row ranges where the column value is not equal to the given value.
    /// column != literal => (min, max) = literal =>
    /// min != literal || literal != max
    virtual PageIndexs notEq(const DB::Field & value) const = 0;

    /// \brief Returns the row ranges where the column value is equal to the given value.
    /// column == literal => literal not in (min, max) =>
    ///  min <= literal && literal <= max
    virtual PageIndexs eq(const DB::Field & value) const = 0;

    /// \brief Returns the row ranges where the column value is greater than the given value.
    /// column > literal
    virtual PageIndexs gt(const DB::Field & value) const = 0;

    /// column >= literal
    virtual PageIndexs gtEg(const DB::Field & value) const = 0;

    /// \brief Returns the row ranges where the column value is less than the given value.
    /// column < literal
    virtual PageIndexs lt(const DB::Field & value) const = 0;

    /// column <= literal
    virtual PageIndexs ltEg(const DB::Field & value) const = 0;

    virtual PageIndexs in(const DB::ColumnPtr & column) const = 0;

    //TODO: parameters
    static ColumnIndexPtr create(
        const parquet::ColumnDescriptor * descr,
        const std::shared_ptr<parquet::ColumnIndex> & column_index,
        const std::shared_ptr<parquet::OffsetIndex> & offset_index);
};

template <typename DType>
class TypedColumnIndex : public ColumnIndex
{
public:
    using T = typename DType::c_type;
};

using ColumnIndexInt64 = TypedColumnIndex<parquet::Int64Type>;
using ColumnIndexInt32 = TypedColumnIndex<parquet::Int32Type>;

class ColumnIndexFilter
{
public:
    using RPN = std::vector<DB::GlutenParquetColumnIndexFilter::RPNElement>;
    using AtomMap = std::unordered_map<std::string, bool (*)(DB::GlutenParquetColumnIndexFilter::RPNElement & out, const DB::Field & value)>;
    static const AtomMap atom_map;

    /// Construct key condition from ActionsDAG nodes
    ColumnIndexFilter(const DB::ActionsDAG & filter_dag, DB::ContextPtr context);

private:
    static bool extractAtomFromTree(const DB::RPNBuilderTreeNode & node, DB::GlutenParquetColumnIndexFilter::RPNElement & out);
    RPN rpn_;

public:
    RowRanges calculateRowRanges(const ColumnIndexStore & index_store, size_t rowgroup_count) const;
};
}
#endif
