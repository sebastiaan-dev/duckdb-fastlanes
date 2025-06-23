#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "fls/encoder/materializer.hpp"

namespace duckdb {

// /**
//  * @brief Class for converting DuckDB vectors to FastLanes columns
//  *
//  * This class provides utilities for materializing DuckDB data into FastLanes format.
//  */
// class FastLanesMaterializer {
// public:
//     /**
//      * @brief Convert a DuckDB Vector to a FastLanes Column
//      *
//      * @param vector The DuckDB Vector to convert
//      * @param type The LogicalType of the vector
//      * @return fastlanes::Column The resulting FastLanes Column
//      */
//     static fastlanes::Column ConvertVectorToColumn(const Vector &vector, const LogicalType &type);
//
//     /**
//      * @brief Convert a DuckDB DataChunk to a FastLanes Batch
//      *
//      * @param chunk The DuckDB DataChunk to convert
//      * @return fastlanes::Batch The resulting FastLanes Batch
//      */
//     static fastlanes::Batch ConvertChunkToBatch(const DataChunk &chunk);
//
// private:
//     // Helper methods for specific types
//     static void MaterializePrimitive(const Vector &vector, const LogicalType &type, fastlanes::Column &column);
//     static void MaterializeString(const Vector &vector, fastlanes::Column &column);
//     static void MaterializeList(const Vector &vector, const LogicalType &type, fastlanes::Column &column);
//     static void MaterializeStruct(const Vector &vector, const LogicalType &type, fastlanes::Column &column);
//     static void MaterializeMap(const Vector &vector, const LogicalType &type, fastlanes::Column &column);
// };

} // namespace duckdb
