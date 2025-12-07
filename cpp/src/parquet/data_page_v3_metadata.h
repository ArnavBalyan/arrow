// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "parquet/platform.h"
#include "parquet/types.h"

namespace parquet {

/// \brief Describes a single encoding stage within an encoding pipeline.
struct PARQUET_EXPORT EncodingStageDescriptor {
  EncodingStageDescriptor() = default;
  EncodingStageDescriptor(Encoding::type enc, std::string metadata)
      : encoding(enc), metadata(std::move(metadata)) {}

  Encoding::type encoding = Encoding::UNDEFINED;
  std::string metadata;
};

/// \brief Describes the sequence of encoding stages applied to a stream.
struct PARQUET_EXPORT EncodingPipelineDescriptor {
  bool empty() const { return stages.empty(); }

  std::vector<EncodingStageDescriptor> stages;
};

/// \brief Encapsulates the opaque metadata stored in format::DataPageHeaderV3.encoding_metadata.
struct PARQUET_EXPORT DataPageV3Metadata {
  static constexpr uint32_t kMagic = 0x33564750;  // "PGV3" in little endian
  static constexpr uint8_t kVersion = 1;

  int64_t repetition_levels_length = 0;
  int64_t definition_levels_length = 0;
  int64_t values_length = 0;
  EncodingPipelineDescriptor values_pipeline;
};

PARQUET_EXPORT std::string SerializeDataPageV3Metadata(
    const DataPageV3Metadata& metadata);

PARQUET_EXPORT DataPageV3Metadata DeserializeDataPageV3Metadata(
    const std::string& metadata_bytes);

}  // namespace parquet
