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

#include "parquet/data_page_v3_metadata.h"

#include <cstring>
#include <limits>

#include "arrow/util/endian.h"

#include "parquet/exception.h"

namespace parquet {

namespace {

constexpr uint8_t kStageEncodingSize = sizeof(uint32_t);
constexpr uint8_t kStageMetadataLengthSize = sizeof(uint32_t);

void AppendLittleEndian32(std::string* out, uint32_t value) {
  const uint32_t le_value = ::arrow::bit_util::ToLittleEndian(value);
  out->append(reinterpret_cast<const char*>(&le_value), sizeof(le_value));
}

void AppendLittleEndian64(std::string* out, uint64_t value) {
  const uint64_t le_value = ::arrow::bit_util::ToLittleEndian(value);
  out->append(reinterpret_cast<const char*>(&le_value), sizeof(le_value));
}

uint32_t LoadLittleEndian32(const uint8_t*& data, const uint8_t* end) {
  if (ARROW_PREDICT_FALSE(end - data < static_cast<ptrdiff_t>(sizeof(uint32_t)))) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (truncated i32)");
  }
  uint32_t value;
  memcpy(&value, data, sizeof(uint32_t));
  data += sizeof(uint32_t);
  return ::arrow::bit_util::FromLittleEndian(value);
}

uint64_t LoadLittleEndian64(const uint8_t*& data, const uint8_t* end) {
  if (ARROW_PREDICT_FALSE(end - data < static_cast<ptrdiff_t>(sizeof(uint64_t)))) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (truncated i64)");
  }
  uint64_t value;
  memcpy(&value, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  return ::arrow::bit_util::FromLittleEndian(value);
}

EncodingStageDescriptor ParseStage(const uint8_t*& data, const uint8_t* end) {
  if (ARROW_PREDICT_FALSE(end - data <
                          static_cast<ptrdiff_t>(kStageEncodingSize +
                                                 kStageMetadataLengthSize))) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (truncated stage)");
  }

  uint32_t encoding_raw = LoadLittleEndian32(data, end);
  auto encoding = static_cast<Encoding::type>(encoding_raw);

  uint32_t metadata_length = LoadLittleEndian32(data, end);
  if (ARROW_PREDICT_FALSE(end - data < static_cast<ptrdiff_t>(metadata_length))) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (stage metadata oob)");
  }
  std::string metadata;
  metadata.resize(metadata_length);
  if (metadata_length > 0) {
    memcpy(metadata.data(), data, metadata_length);
    data += metadata_length;
  }
  return EncodingStageDescriptor{encoding, std::move(metadata)};
}

}  // namespace

std::string SerializeDataPageV3Metadata(const DataPageV3Metadata& metadata) {
  if (metadata.repetition_levels_length < 0 || metadata.definition_levels_length < 0 ||
      metadata.values_length < 0) {
    throw ParquetException("Negative lengths cannot be stored in DataPageV3 metadata");
  }

  if (metadata.values_pipeline.stages.size() >
      static_cast<size_t>(std::numeric_limits<uint8_t>::max())) {
    throw ParquetException("Too many encoding stages in DataPageV3 metadata");
  }

  std::string out;
  out.reserve(32);
  AppendLittleEndian32(&out, DataPageV3Metadata::kMagic);
  out.push_back(DataPageV3Metadata::kVersion);
  out.append(3, '\0');  // reserved
  AppendLittleEndian64(&out, static_cast<uint64_t>(metadata.repetition_levels_length));
  AppendLittleEndian64(&out, static_cast<uint64_t>(metadata.definition_levels_length));
  AppendLittleEndian64(&out, static_cast<uint64_t>(metadata.values_length));
  out.push_back(static_cast<uint8_t>(metadata.values_pipeline.stages.size()));

  for (const auto& stage : metadata.values_pipeline.stages) {
    AppendLittleEndian32(&out, static_cast<uint32_t>(stage.encoding));
    if (stage.metadata.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
      throw ParquetException("Stage metadata too large in DataPageV3 metadata");
    }
    AppendLittleEndian32(&out, static_cast<uint32_t>(stage.metadata.size()));
    out.append(stage.metadata);
  }

  return out;
}

DataPageV3Metadata DeserializeDataPageV3Metadata(const std::string& metadata_bytes) {
  if (metadata_bytes.size() <
      static_cast<int64_t>(sizeof(uint32_t) + sizeof(uint8_t) + 3)) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (too short)");
  }

  const uint8_t* data =
      reinterpret_cast<const uint8_t*>(metadata_bytes.data());
  const uint8_t* end = data + metadata_bytes.size();

  uint32_t magic = LoadLittleEndian32(data, end);
  if (magic != DataPageV3Metadata::kMagic) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (bad magic)");
  }

  uint8_t version = *data++;
  if (version != DataPageV3Metadata::kVersion) {
    throw ParquetException("Unsupported DataPageV3 encoding metadata version");
  }
  // Skip reserved bytes
  if (end - data < 3) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (truncated reserved)");
  }
  data += 3;

  DataPageV3Metadata metadata;
  metadata.repetition_levels_length = static_cast<int64_t>(LoadLittleEndian64(data, end));
  metadata.definition_levels_length = static_cast<int64_t>(LoadLittleEndian64(data, end));
  metadata.values_length = static_cast<int64_t>(LoadLittleEndian64(data, end));
  if (metadata.repetition_levels_length < 0 || metadata.definition_levels_length < 0 ||
      metadata.values_length < 0) {
    throw ParquetException("Negative lengths in DataPageV3 encoding metadata");
  }
  if (ARROW_PREDICT_FALSE(data == end)) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (missing stage count)");
  }
  uint8_t stage_count = *data++;
  metadata.values_pipeline.stages.reserve(stage_count);

  for (uint8_t i = 0; i < stage_count; ++i) {
    metadata.values_pipeline.stages.emplace_back(ParseStage(data, end));
  }

  if (data != end) {
    throw ParquetException("Corrupt DataPageV3 encoding metadata (extra bytes)");
  }
  return metadata;
}

}  // namespace parquet
