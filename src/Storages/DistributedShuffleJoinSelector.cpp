#include <Storages/DistributedShuffleJoinSelector.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/createBlockSelector.h>
#include <Storages/DistributedShuffleJoinAnalyzer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}

DistributedShuffleJoinSelector createDistributedShuffleJoinSelector(ClusterPtr cluster, String key_column_name)
{
    if (!cluster)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` selector cannot be created without cluster");

    if (key_column_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` selector cannot be created with empty key column name");

    auto cluster_for_selector = std::move(cluster);
    auto key_column_name_for_selector = std::move(key_column_name);

    return [captured_cluster = std::move(cluster_for_selector), captured_key_column_name = std::move(key_column_name_for_selector)](const Block & block)
    {
        const auto * key_column = block.findByName(captured_key_column_name);
        if (!key_column)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find distributed `shuffle join` key column {} in block with columns {}",
                captured_key_column_name,
                block.dumpNames());
        }

        const auto & slot_to_shard = captured_cluster->getSlotToShard();
        const auto * column = key_column->column.get();

/// NOLINTBEGIN(readability-else-after-return)
#define CREATE_FOR_TYPE(TYPE)                                                                               \
        if (typeid_cast<const DataType##TYPE *>(key_column->type.get()))                                    \
            return createBlockSelector<TYPE>(*column, slot_to_shard);                                       \
        else if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(key_column->type.get())) \
            if (typeid_cast<const DataType##TYPE *>(low_cardinality_type->getDictionaryType().get()))       \
                return createBlockSelector<TYPE>(*column->convertToFullColumnIfLowCardinality(), slot_to_shard);

        CREATE_FOR_TYPE(UInt8)
        CREATE_FOR_TYPE(UInt16)
        CREATE_FOR_TYPE(UInt32)
        CREATE_FOR_TYPE(UInt64)
        CREATE_FOR_TYPE(Int8)
        CREATE_FOR_TYPE(Int16)
        CREATE_FOR_TYPE(Int32)
        CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE
/// NOLINTEND(readability-else-after-return)

        throw Exception(ErrorCodes::TYPE_MISMATCH, "Distributed `shuffle join` key column {} does not have an integer type", captured_key_column_name);
    };
}

DistributedShuffleJoinSelector createDistributedShuffleJoinSelector(
    ClusterPtr cluster,
    const DistributedShuffleJoinInfo & info,
    DistributedShuffleJoinTableSide side)
{
    const auto & key_column_name = side == DistributedShuffleJoinTableSide::Left
        ? info.left_key_column_name
        : info.right_key_column_name;

    return createDistributedShuffleJoinSelector(std::move(cluster), key_column_name);
}

}
