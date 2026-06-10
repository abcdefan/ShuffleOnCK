#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <base/types.h>

#include <cstdint>
#include <optional>

namespace DB
{

struct DistributedShuffleJoinExchangeId
{
    String initial_query_id;
    size_t join_id = 0;
    UInt64 expiration_time_ms = 0;

    String toString() const;

    bool operator==(const DistributedShuffleJoinExchangeId & rhs) const
    {
        return initial_query_id == rhs.initial_query_id && join_id == rhs.join_id;
    }
};

enum class DistributedShuffleJoinTableSide : uint8_t
{
    Left,
    Right,
};

const char * toString(DistributedShuffleJoinTableSide side);

/// Server-visible `Memory` tables used by the table-based `shuffle join` MVP.
///
/// The same table names are created on every target shard. Each target shard
/// stores only the buckets assigned to itself, not a full copy of the input
/// tables.
struct DistributedShuffleJoinTableNames
{
    String database;
    String left_table;
    String right_table;

    String getTableName(DistributedShuffleJoinTableSide side) const;
    String getQualifiedTableName(DistributedShuffleJoinTableSide side) const;
};

String makeDistributedShuffleJoinTableNamePrefix(const DistributedShuffleJoinExchangeId & id);
std::optional<UInt64> tryGetDistributedShuffleJoinTableExpirationTimeMs(const String & table_name);
DistributedShuffleJoinTableNames createDistributedShuffleJoinTableNames(String database, const DistributedShuffleJoinExchangeId & id);
Block createDistributedShuffleJoinTableHeader(const NamesAndTypes & columns);

String createDistributedShuffleJoinMemoryTableQuery(
    const DistributedShuffleJoinTableNames & table_names,
    DistributedShuffleJoinTableSide side,
    const Block & header);

String createDistributedShuffleJoinMemoryTableQuery(
    String qualified_table_name,
    const Block & header);

String dropDistributedShuffleJoinTableQuery(
    const DistributedShuffleJoinTableNames & table_names,
    DistributedShuffleJoinTableSide side);

String dropDistributedShuffleJoinTableQuery(String qualified_table_name);

}
