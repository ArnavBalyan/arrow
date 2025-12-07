#pragma once

#include <cstdint>
#include <vector>

#include "parquet/platform.h"

namespace parquet {

/// \brief Enumerates the stages that may participate in a composite encoding pipeline.
enum class CompositeStage : uint8_t {
  /// Applies delta encoding to signed integer streams.
  DELTA_BINARY = 0,
  /// Applies run-length encoding to integer streams.
  RLE = 1,
};

/// \brief Describes the ordered stages that make up a composite encoding.
struct PARQUET_EXPORT CompositeEncodingSpec {
  std::vector<CompositeStage> stages;

  bool empty() const { return stages.empty(); }
};

}  // namespace parquet
