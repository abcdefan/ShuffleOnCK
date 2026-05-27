#include <Storages/DistributedShuffleJoinTables.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <DataTypes/IDataType.h>
#include <fmt/format.h>

#include <charconv>
#include <cctype>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

String sanitizeIdentifierFragment(const String & value)
{
    String result;
    result.reserve(value.size());

    for (char character : value)
    {
        const auto unsigned_character = static_cast<unsigned char>(character);
        if (std::isalnum(unsigned_character) || character == '_')
            result += character;
        else
            result += '_';
    }

    while (!result.empty() && result.back() == '_')
        result.pop_back();

    if (result.empty())
        return "query";

    return result;
}

String formatColumnsForCreateQuery(const Block & header)
{
    if (header.columns() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` Memory table cannot be created with empty header");

    String result;
    for (size_t column_index = 0; column_index < header.columns(); ++column_index)
    {
        const auto & column = header.getByPosition(column_index);

        if (column.name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` Memory table column {} has empty name", column_index);

        if (!column.type)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed `shuffle join` Memory table column {} does not have a type",
                column.name);

        if (!result.empty())
            result += ", ";

        result += backQuoteIfNeed(column.name);
        result += " ";
        result += column.type->getName();
    }

    return result;
}

}

String DistributedShuffleJoinExchangeId::toString() const
{
    return fmt::format("{}:{}", initial_query_id, join_id);
}

const char * toString(DistributedShuffleJoinTableSide side)
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return "left";

    return "right";
}

String DistributedShuffleJoinTableNames::getTableName(DistributedShuffleJoinTableSide side) const
{
    if (side == DistributedShuffleJoinTableSide::Left)
        return left_table;

    return right_table;
}

String DistributedShuffleJoinTableNames::getQualifiedTableName(DistributedShuffleJoinTableSide side) const
{
    if (database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` Memory table database cannot be empty");

    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(getTableName(side));
}

String makeDistributedShuffleJoinTableNamePrefix(const DistributedShuffleJoinExchangeId & id)
{
    if (id.initial_query_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` table name cannot be created with empty initial query id");

    auto prefix = fmt::format("_shuffle_{}_{}", sanitizeIdentifierFragment(id.initial_query_id), id.join_id);
    if (id.expiration_time_ms != 0)
        prefix += fmt::format("_expires_{}", id.expiration_time_ms);

    return prefix;
}

std::optional<UInt64> tryGetDistributedShuffleJoinTableExpirationTimeMs(const String & table_name)
{
    static constexpr std::string_view prefix = "_shuffle_";
    static constexpr std::string_view expiration_marker = "_expires_";

    if (!table_name.starts_with(prefix))
        return {};

    const auto expiration_pos = table_name.rfind(expiration_marker);
    if (expiration_pos == String::npos)
        return {};

    const auto expiration_begin = expiration_pos + expiration_marker.size();
    const auto expiration_end = table_name.find('_', expiration_begin);
    if (expiration_end == String::npos)
        return {};

    const auto side = std::string_view(table_name).substr(expiration_end);
    if (side != "_left" && side != "_right")
        return {};

    UInt64 expiration_time_ms = 0;
    const auto * begin = table_name.data() + expiration_begin;
    const auto * end = table_name.data() + expiration_end;
    const auto parse_result = std::from_chars(begin, end, expiration_time_ms);
    if (parse_result.ec != std::errc{} || parse_result.ptr != end)
        return {};

    return expiration_time_ms;
}

DistributedShuffleJoinTableNames createDistributedShuffleJoinTableNames(String database, const DistributedShuffleJoinExchangeId & id)
{
    if (database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` Memory table database cannot be empty");

    auto prefix = makeDistributedShuffleJoinTableNamePrefix(id);
    return DistributedShuffleJoinTableNames
    {
        .database = std::move(database),
        .left_table = prefix + "_left",
        .right_table = prefix + "_right",
    };
}

Block createDistributedShuffleJoinTableHeader(const NamesAndTypes & columns)
{
    if (columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` Memory table cannot be created with empty columns");

    Block header;
    for (const auto & column : columns)
    {
        if (column.name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Distributed `shuffle join` Memory table column has empty name");

        if (!column.type)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Distributed `shuffle join` Memory table column {} does not have a type",
                column.name);

        header.insert(ColumnWithTypeAndName(column.type, column.name));
    }

    return header;
}

String createDistributedShuffleJoinMemoryTableQuery(
    const DistributedShuffleJoinTableNames & table_names,
    DistributedShuffleJoinTableSide side,
    const Block & header)
{
    return fmt::format(
        "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = Memory",
        table_names.getQualifiedTableName(side),
        formatColumnsForCreateQuery(header));
}

String dropDistributedShuffleJoinTableQuery(
    const DistributedShuffleJoinTableNames & table_names,
    DistributedShuffleJoinTableSide side)
{
    return fmt::format("DROP TABLE IF EXISTS {}", table_names.getQualifiedTableName(side));
}

}
