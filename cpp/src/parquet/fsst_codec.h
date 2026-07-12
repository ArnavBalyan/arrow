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

#include <cstddef>
#include <cstdint>

#include "parquet/fsst/fsst.h"
#include "parquet/platform.h"

namespace parquet {

constexpr int kFsstSymbolTableHeaderSize = 1 + 8;

constexpr int kMaxFsstSymbolCount = 255;

constexpr int kFsstBitsPerByte = 8;
constexpr int kFsstMaxSymbolLength = 8;
constexpr int kFsstEndOffsetSize = 4;
constexpr int kFsstBitsPerLenOffset = 4;
constexpr int kFsstDataPageHeaderSize = kFsstBitsPerLenOffset + 1;
constexpr int kFsstDecodeFastPathCodes = 4;
constexpr int kFsstDecodeFastPathHeadroom =
    kFsstMaxSymbolLength * kFsstDecodeFastPathCodes;
constexpr int kFsstWorstCaseExpansion = 2;
constexpr int kFsstCompressPadding = 7;

class PARQUET_EXPORT FsstCodec {
 public:
  FsstCodec();
  ~FsstCodec();

  FsstCodec(const FsstCodec&) = delete;
  FsstCodec& operator=(const FsstCodec&) = delete;

  bool Train(size_t n, const size_t* lengths, const uint8_t* const* strings);

  size_t CompressBatch(size_t nstrings, const size_t* lenIn,
                       const uint8_t* const* strIn, size_t outsize,
                       uint8_t* output, size_t* lenOut, uint8_t** strOut);

  int symbol_count() const { return symbol_count_; }

  int SerializedSymbolTableSize() const;

  int SerializeSymbolTable(uint8_t* buffer, int buffer_len) const;

  static bool DeserializeSymbolTable(const uint8_t* buffer, int len,
                                     fsst_decoder_t* decoder);

  bool trained() const { return trained_; }

 private:
  fsst_encoder_t* encoder_ = nullptr;
  fsst_decoder_t decoder_{};
  uint8_t len_histo_[8] = {};
  int symbol_count_ = 0;
  bool trained_ = false;
};

}  
