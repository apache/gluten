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

#include <city.h>
#include <base/types.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wused-but-marked-unused"
#endif

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionsHashing.h>
#include <Common/NaNUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace local_engine
{

template <typename T>
requires std::is_integral_v<T>
struct IntHashPromotion
{
    static constexpr bool is_signed = is_signed_v<T>;
    static constexpr size_t size = std::max<size_t>(4, sizeof(T));

    using Type = typename DB::NumberTraits::Construct<is_signed, false, size>::Type;
    static constexpr bool need_promotion_v = sizeof(T) < 4;
};


inline String toHexString(const char * buf, size_t length)
{
    String res(length * 2, '\0');
    char * out = res.data();
    for (size_t i = 0; i < length; ++i)
    {
        writeHexByteUppercase(buf[i], out);
        out += 2;
    }
    return res;
}

DECLARE_MULTITARGET_CODE(

template <typename Impl>
class SparkFunctionAnyHash : public DB::IFunction
{
public:
    static constexpr auto name = Impl::name;

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

private:
    using ToType = typename Impl::ReturnType;

    static ToType applyGeneric(const DB::Field & field, UInt64 seed, const DB::DataTypePtr & type)
    {
        /// Do nothing when field is null
        if (field.isNull())
            return seed;

        DB::DataTypePtr non_nullable_type = removeNullable(type);
        DB::WhichDataType which(non_nullable_type);
        if (which.isNothing())
            return seed;
        else if (which.isUInt8())
            return applyNumber<UInt8>(field.safeGet<UInt8>(), seed);
        else if (which.isUInt16())
            return applyNumber<UInt16>(field.safeGet<UInt16>(), seed);
        else if (which.isUInt32())
            return applyNumber<UInt32>(field.safeGet<UInt32>(), seed);
        else if (which.isUInt64())
            return applyNumber<UInt64>(field.safeGet<UInt64>(), seed);
        else if (which.isInt8())
            return applyNumber<Int8>(field.safeGet<Int8>(), seed);
        else if (which.isInt16())
            return applyNumber<Int16>(field.safeGet<Int16>(), seed);
        else if (which.isInt32())
            return applyNumber<Int32>(field.safeGet<Int32>(), seed);
        else if (which.isInt64())
            return applyNumber<Int64>(field.safeGet<Int64>(), seed);
        else if (which.isFloat32())
            return applyNumber<Float32>(field.safeGet<Float32>(), seed);
        else if (which.isFloat64())
            return applyNumber<Float64>(field.safeGet<Float64>(), seed);
        else if (which.isDate())
            return applyNumber<UInt16>(field.safeGet<UInt16>(), seed);
        else if (which.isDate32())
            return applyNumber<Int32>(field.safeGet<Int32>(), seed);
        else if (which.isDateTime())
            return applyNumber<UInt32>(field.safeGet<UInt32>(), seed);
        else if (which.isDateTime64())
            return applyDecimal<DB::DateTime64>(field.safeGet<DB::DateTime64>(), seed);
        else if (which.isDecimal32())
            return applyDecimal<DB::Decimal32>(field.safeGet<DB::Decimal32>(), seed);
        else if (which.isDecimal64())
            return applyDecimal<DB::Decimal64>(field.safeGet<DB::Decimal64>(), seed);
        else if (which.isDecimal128())
            return applyDecimal<DB::Decimal128>(field.safeGet<DB::Decimal128>(), seed);
        else if (which.isStringOrFixedString())
        {
            const String & str = field.safeGet<String>();
            return applyUnsafeBytes(str.data(), str.size(), seed);
        }
        else if (which.isTuple())
        {
            const auto * tuple_type = checkAndGetDataType<DB::DataTypeTuple>(non_nullable_type.get());
            assert(tuple_type);

            const auto & elements = tuple_type->getElements();
            const DB::Tuple & tuple = field.safeGet<DB::Tuple>();
            assert(tuple.size() == elements.size());

            for (size_t i = 0; i < elements.size(); ++i)
            {
                seed = applyGeneric(tuple[i], seed, elements[i]);
            }
            return seed;
        }
        else if (which.isArray())
        {
            const auto * array_type = checkAndGetDataType<DB::DataTypeArray>(non_nullable_type.get());
            assert(array_type);

            const auto & nested_type = array_type->getNestedType();
            const DB::Array & array = field.safeGet<DB::Array>();
            for (size_t i=0; i < array.size(); ++i)
            {
                seed = applyGeneric(array[i], seed, nested_type);
            }
            return seed;
        }
        else
        {
            /// Note: No need to implement for big int type in gluten
            /// Note: No need to implement for uuid/ipv4/ipv6/enum* type in gluten
            /// Note: No need to implement for decimal256 type in gluten
            /// Note: No need to implement for map type as long as spark.sql.legacy.allowHashOnMapType is false(default)
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unsupported type {}", type->getName());
        }
    }

    static ToType applyUnsafeBytes(const char * begin, size_t size, UInt64 seed)
    {
        return Impl::apply(begin, size, seed);
    }

    template <typename T>
        requires std::is_arithmetic_v<T>
    static ToType applyNumber(T n, UInt64 seed)
    {
        if constexpr (std::is_integral_v<T>)
        {
            if constexpr (IntHashPromotion<T>::need_promotion_v)
            {
                using PromotedType = typename IntHashPromotion<T>::Type;
                PromotedType v = n;
                return Impl::apply(reinterpret_cast<const char *>(&v), sizeof(v), seed);
            }
            else
                return Impl::apply(reinterpret_cast<const char *>(&n), sizeof(n), seed);
        }
        else
        {
            if constexpr (std::is_same_v<T, Float32>)
            {
                if (n == -0.0f || isNaN(n)) [[unlikely]]
                    return applyNumber<Int32>(0, seed);
                else
                    return Impl::apply(reinterpret_cast<const char *>(&n), sizeof(n), seed);
            }
            else
            {
                if (n == -0.0 || isNaN(n)) [[unlikely]]
                    return applyNumber<Int64>(0, seed);
                else
                    return Impl::apply(reinterpret_cast<const char *>(&n), sizeof(n), seed);
            }
        }
    }

    template <typename T>
        requires DB::is_decimal<T>
    static ToType applyDecimal(const T & n, UInt64 seed)
    {
        using NativeType = typename T::NativeType;

        if constexpr (sizeof(NativeType) <= 8)
        {
            Int64 v = n.value;
            return Impl::apply(reinterpret_cast<const char *>(&v), sizeof(v), seed);
        }
        else
        {
            using base_type = typename NativeType::base_type;

            NativeType v = n.value;

            /// Calculate leading zeros
            constexpr size_t item_count = std::size(v.items);
            constexpr size_t total_bytes = sizeof(base_type) * item_count;
            bool negative = v < 0;
            size_t leading_zeros = 0;
            for (size_t i = 0; i < item_count; ++i)
            {
                base_type item = v.items[item_count - 1 - i];
                base_type temp = negative ? ~item : item;
                size_t curr_leading_zeros = getLeadingZeroBits(temp);
                leading_zeros += curr_leading_zeros;

                if (curr_leading_zeros != sizeof(base_type) * 8)
                    break;
            }

            size_t offset = total_bytes - (total_bytes * 8 - leading_zeros + 8) / 8;

            /// Convert v to big-endian style
            for (size_t i = 0; i < item_count; ++i)
                v.items[i] = __builtin_bswap64(v.items[i]);
            for (size_t i = 0; i < item_count / 2; ++i)
                std::swap(v.items[i], v.items[std::size(v.items) -1 - i]);

            /// Calculate hash(refer to https://docs.oracle.com/javase/8/docs/api/java/math/BigInteger.html#toByteArray)
            const char * buffer = reinterpret_cast<const char *>(&v.items[0]) + offset;
            size_t length = item_count * sizeof(base_type) - offset;
            return Impl::apply(buffer, length, seed);
        }
    }

    void executeGeneric(
        const DB::IDataType * from_type,
        bool from_const,
        const DB::IColumn * data_column,
        const DB::NullMap * null_map,
        typename DB::ColumnVector<ToType>::Container & vec_to) const
    {
        size_t size = vec_to.size();
        if (!from_const)
        {
            for (size_t i = 0; i < size; ++i)
                if (!null_map || !(*null_map)[i]) [[likely]]
                    vec_to[i] = applyGeneric((*data_column)[i], vec_to[i], from_type->shared_from_this());
        }
        else if (!null_map || !(*null_map)[0]) [[likely]]
        {
            auto value = (*data_column)[0];
            for (size_t i = 0; i < size; ++i)
                vec_to[i] = applyGeneric(value, vec_to[i], from_type->shared_from_this());
        }
    }

    template <typename FromType>
    void executeNumberType(
        bool from_const, const DB::IColumn * data_column, const DB::NullMap * null_map, typename DB::ColumnVector<ToType>::Container & vec_to) const
    {
        using ColVecType = DB::ColumnVectorOrDecimal<FromType>;

        const ColVecType * col_from = checkAndGetColumn<ColVecType>(data_column);
        if (!col_from)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", data_column->getName(), getName());

        size_t size = vec_to.size();
        const typename ColVecType::Container & vec_from = col_from->getData();

        auto update_hash = [&](const FromType & value, ToType & to)
        {
            if constexpr (std::is_arithmetic_v<FromType>)
                to = applyNumber(value, to);
            else if constexpr (DB::is_decimal<FromType>)
                to = applyDecimal(value, to);
            else
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", data_column->getName(), getName());
        };

        if (!from_const)
        {
            for (size_t i = 0; i < size; ++i)
            {
                if (!null_map || !(*null_map)[i]) [[likely]]
                    update_hash(vec_from[i], vec_to[i]);
            }
        }
        else
        {
            if (!null_map || !(*null_map)[0]) [[likely]]
            {
                auto value = vec_from[0];
                for (size_t i = 0; i < size; ++i)
                    update_hash(value, vec_to[i]);
            }
        }
    }

    void executeFixedString(bool from_const, const DB::IColumn * data_column, const DB::NullMap * null_map, typename DB::ColumnVector<ToType>::Container & vec_to) const
    {
        const DB::ColumnFixedString * col_from_fixed = checkAndGetColumn<DB::ColumnFixedString>(data_column);
        if (!col_from_fixed)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", data_column->getName(), getName());

        size_t size = vec_to.size();
        if (!from_const)
        {
            const typename DB::ColumnString::Chars & data = col_from_fixed->getChars();
            size_t n = col_from_fixed->getN();
            for (size_t i = 0; i < size; ++i)
            {
                if (!null_map || !(*null_map)[i]) [[likely]]
                    vec_to[i] = applyUnsafeBytes(reinterpret_cast<const char *>(&data[i * n]), n, vec_to[i]);
            }
        }
        else
        {
            if (!null_map || !(*null_map)[0]) [[likely]]
            {
                StringRef ref = col_from_fixed->getDataAt(0);

                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = applyUnsafeBytes(ref.data, ref.size, vec_to[i]);
            }
        }
    }

    void executeString(bool from_const, const DB::IColumn * data_column, const DB::NullMap * null_map, typename DB::ColumnVector<ToType>::Container & vec_to) const
    {
        const DB::ColumnString * col_from = checkAndGetColumn<DB::ColumnString>(data_column);
        if (!col_from)
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", data_column->getName(), getName());

        size_t size = vec_to.size();
        if (!from_const)
        {
            const typename DB::ColumnString::Chars & data = col_from->getChars();
            const typename DB::ColumnString::Offsets & offsets = col_from->getOffsets();

            DB::ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                if (!null_map || !(*null_map)[i]) [[likely]]
                    vec_to[i] = applyUnsafeBytes(
                        reinterpret_cast<const char *>(&data[current_offset]), offsets[i] - current_offset, vec_to[i]);

                current_offset = offsets[i];
            }
        }
        else
        {
            if (!null_map || !(*null_map)[0]) [[likely]]
            {
                StringRef ref = col_from->getDataAt(0);

                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = applyUnsafeBytes(ref.data, ref.size, vec_to[i]);
            }
        }
    }

    void executeAny(const DB::IDataType * from_type, const DB::IColumn * column, typename DB::ColumnVector<ToType>::Container & vec_to) const
    {
        if (column->size() != vec_to.size())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Argument column '{}' size {} doesn't match result column size {} of function {}",
                    column->getName(), column->size(), vec_to.size(), getName());

        const DB::NullMap * null_map = nullptr;
        const DB::IColumn * data_column = column;
        bool from_const = false;

        if (isColumnConst(*column))
        {
            from_const = true;
            data_column = &assert_cast<const DB::ColumnConst &>(*column).getDataColumn();
        }

        if (const DB::ColumnNullable * col_nullable = checkAndGetColumn<DB::ColumnNullable>(data_column))
        {
            null_map = &col_nullable->getNullMapData();
            data_column = &col_nullable->getNestedColumn();
        }

        DB::WhichDataType which(removeNullable(from_type->shared_from_this()));

        /// Skip column with type Nullable(Nothing)
        if (which.isNothing())
            ;
        else if (which.isUInt8())
            executeNumberType<UInt8>(from_const, data_column, null_map, vec_to);
        else if (which.isUInt16())
            executeNumberType<UInt16>(from_const, data_column, null_map, vec_to);
        else if (which.isUInt32())
            executeNumberType<UInt32>(from_const, data_column, null_map, vec_to);
        else if (which.isUInt64())
            executeNumberType<UInt64>(from_const, data_column, null_map, vec_to);
        else if (which.isInt8())
            executeNumberType<Int8>(from_const, data_column, null_map, vec_to);
        else if (which.isInt16())
            executeNumberType<Int16>(from_const, data_column, null_map, vec_to);
        else if (which.isInt32())
            executeNumberType<Int32>(from_const, data_column, null_map, vec_to);
        else if (which.isInt64())
            executeNumberType<Int64>(from_const, data_column, null_map, vec_to);
        else if (which.isFloat32())
            executeNumberType<Float32>(from_const, data_column, null_map, vec_to);
        else if (which.isFloat64())
            executeNumberType<Float64>(from_const, data_column, null_map, vec_to);
        else if (which.isDate())
            executeNumberType<UInt16>(from_const, data_column, null_map, vec_to);
        else if (which.isDate32())
            executeNumberType<Int32>(from_const, data_column, null_map, vec_to);
        else if (which.isDateTime())
            executeNumberType<UInt32>(from_const, data_column, null_map, vec_to);
        else if (which.isDateTime64())
            executeNumberType<DB::DateTime64>(from_const, data_column, null_map, vec_to);
        else if (which.isDecimal32())
            executeNumberType<DB::Decimal32>(from_const, data_column, null_map, vec_to);
        else if (which.isDecimal64())
            executeNumberType<DB::Decimal64>(from_const, data_column, null_map, vec_to);
        else if (which.isDecimal128())
            executeNumberType<DB::Decimal128>(from_const, data_column, null_map, vec_to);
        else if (which.isString())
            executeString(from_const, data_column, null_map, vec_to);
        else if (which.isFixedString())
            executeFixedString(from_const, data_column, null_map, vec_to);
        else if (which.isArray())
            executeGeneric(from_type, from_const, data_column, null_map, vec_to);
        else if (which.isTuple())
            executeGeneric(from_type, from_const, data_column, null_map, vec_to);
        else
        {
            /// Note: No need to implement for big int type in gluten
            /// Note: No need to implement for uuid/ipv4/ipv6/enum* type in gluten
            /// Note: No need to implement for decimal256 type in gluten
            /// Note: No need to implement for map type as long as spark.sql.legacy.allowHashOnMapType is false(default)
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Function {} hasn't supported type {}", getName(), from_type->getName());
        }
    }

    void executeForArgument(
        const DB::IDataType * type, const DB::IColumn * column, typename DB::ColumnVector<ToType>::Container & vec_to) const
    {
        executeAny(type, column, vec_to);
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DB::DataTypeNumber<ToType>>();
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = DB::ColumnVector<ToType>::create(input_rows_count);

        /// Initial seed is always 42
        typename DB::ColumnVector<ToType>::Container & vec_to = col_to->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            vec_to[i] = 42;

        /// The function supports arbitrary number of arguments of arbitrary types.
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & col = arguments[i];
            executeForArgument(col.type.get(), col.column.get(), vec_to);
        }

        return col_to;
    }


};

) // DECLARE_MULTITARGET_CODE

template <typename Impl>
class SparkFunctionAnyHash : public TargetSpecific::Default::SparkFunctionAnyHash<Impl>
{
public:
    explicit SparkFunctionAnyHash(DB::ContextPtr context) : selector(context)
    {
        selector.registerImplementation<DB::TargetArch::Default, TargetSpecific::Default::SparkFunctionAnyHash<Impl>>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<DB::TargetArch::AVX2, TargetSpecific::AVX2::SparkFunctionAnyHash<Impl>>();
        selector.registerImplementation<DB::TargetArch::AVX512F, TargetSpecific::AVX512F::SparkFunctionAnyHash<Impl>>();
#endif
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static DB::FunctionPtr create(DB::ContextPtr context) { return std::make_shared<SparkFunctionAnyHash>(context); }

private:
    DB::ImplementationSelector<DB::IFunction> selector;
};


/// For spark compatiability of ClickHouse.
/// The difference between spark xxhash64 and CH xxHash64
/// In Spark, the seed is 42
/// In CH, the seed is 0. So we need to add new impl ImplXxHash64Spark in CH with seed = 42.
struct SparkImplXxHash64
{
    static constexpr auto name = "sparkXxHash64";
    using ReturnType = UInt64;
    static auto apply(const char * s, size_t len, UInt64 seed) { return XXH_INLINE_XXH64(s, len, seed); }
};


/// Block read - if your platform needs to do endian-swapping or can only
/// handle aligned reads, do the conversion here
static ALWAYS_INLINE uint32_t getblock32(const uint32_t * p, int i)
{
    return p[i];
}

static ALWAYS_INLINE uint32_t rotl32(uint32_t x, int8_t r)
{
    return (x << r) | (x >> (32 - r));
}

static void SparkMurmurHash3_32_Impl(const void * key, size_t len, uint32_t seed, void * out)
{
    const uint8_t * data = static_cast<const uint8_t *>(key);
    const int nblocks = static_cast<int>(len >> 2);

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    /// body
    const uint32_t * blocks = reinterpret_cast<const uint32_t *>(data + nblocks * 4);

    for (int i = -nblocks; i; i++)
    {
        uint32_t k1 = getblock32(blocks, i);

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
    }

    /// tail
    const uint8_t * tail = (data + nblocks * 4);
    uint32_t k1 = 0;
    while (tail != data + len)
    {
        /// Notice: we must use int8_t here, to compatible with all platforms (x86, arm...).
        k1 = static_cast<int8_t>(*tail);

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        ++tail;
    }

    /// finalization
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    /// output
    *static_cast<uint32_t *>(out) = h1;
}

/// For spark compatiability of ClickHouse.
/// The difference between spark hash and CH murmurHash3_32
/// 1. They calculate hash functions with different seeds
/// 2. Spark current impl is not right, but it is not fixed for backward compatiability. See: https://issues.apache.org/jira/browse/SPARK-23381
struct SparkMurmurHash3_32
{
    static constexpr auto name = "sparkMurmurHash3_32";
    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size, UInt64 seed)
    {
        union
        {
            UInt32 h;
            char bytes[sizeof(h)];
        };
        SparkMurmurHash3_32_Impl(data, size, static_cast<UInt32>(seed), bytes);
        return h;
    }
};

using SparkFunctionXxHash64 = SparkFunctionAnyHash<SparkImplXxHash64>;
using SparkFunctionMurmurHash3_32 = SparkFunctionAnyHash<SparkMurmurHash3_32>;

}
