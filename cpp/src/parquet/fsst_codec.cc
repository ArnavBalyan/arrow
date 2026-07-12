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

#include "parquet/fsst_codec.h"

#include <cstring>

#include "parquet/exception.h"

namespace parquet {

FsstCodec::FsstCodec() { std::memset(&decoder_, 0, sizeof(decoder_)); }

FsstCodec::~FsstCodec() {
  if (encoder_) {
    fsst_destroy(encoder_);
  }
}

bool FsstCodec::Train(size_t n, const size_t* lengths,
                      const uint8_t* const* strings) {
  if (encoder_) {
    fsst_destroy(encoder_);
    encoder_ = nullptr;
  }
  trained_ = false;
  symbol_count_ = 0;
  std::memset(len_histo_, 0, sizeof(len_histo_));
  if (n == 0) return false;

  encoder_ = fsst_create(n, lengths,
                          const_cast<const unsigned char**>(
                              reinterpret_cast<const unsigned char* const*>(strings)),
                          0);
  if (!encoder_) return false;

  decoder_ = fsst_decoder(encoder_);

  uint8_t header[FSST_MAXHEADER];
  fsst_export(encoder_, header);
  std::memcpy(len_histo_, header + 9, 8);
  symbol_count_ = 0;
  for (int i = 0; i < 8; ++i) symbol_count_ += len_histo_[i];

  trained_ = true;
  return true;
}

size_t FsstCodec::CompressBatch(size_t nstrings, const size_t* lenIn,
                                const uint8_t* const* strIn, size_t outsize,
                                uint8_t* output, size_t* lenOut,
                                uint8_t** strOut) {
  if (!encoder_) throw ParquetException("FSST codec not trained");
  return fsst_compress(encoder_, nstrings, lenIn,
                       const_cast<const unsigned char**>(
                           reinterpret_cast<const unsigned char* const*>(strIn)),
                       outsize, output, lenOut, strOut);
}

int FsstCodec::SerializedSymbolTableSize() const {
  if (!trained_) return 0;
  int total_data = 0;
  for (int i = 0; i < 8; ++i) total_data += len_histo_[i] * (i + 1);
  return kFsstSymbolTableHeaderSize + total_data;
}

int FsstCodec::SerializeSymbolTable(uint8_t* buffer, int buffer_len) const {
  const int total = SerializedSymbolTableSize();
  if (!trained_) return -1;
  if (buffer == nullptr) return total;
  if (buffer_len < total) return -1;

  buffer[0] = static_cast<uint8_t>(symbol_count_);

  uint8_t wire_histogram[8] = {len_histo_[1], len_histo_[2], len_histo_[3], len_histo_[4],
                                len_histo_[5], len_histo_[6], len_histo_[7], len_histo_[0]};
  std::memcpy(buffer + 1, wire_histogram, 8);

  int offset = kFsstSymbolTableHeaderSize;
  for (int i = 0; i < symbol_count_; ++i) {
    int L = decoder_.len[i];
    std::memcpy(buffer + offset, &decoder_.symbol[i], L);
    offset += L;
  }

  return total;
}

bool FsstCodec::DeserializeSymbolTable(const uint8_t* buffer, int len,
                                       fsst_decoder_t* decoder) {
  if (!buffer || !decoder) return false;
  if (len < kFsstSymbolTableHeaderSize) return false;

  const uint8_t symbol_count = buffer[0];
  if (symbol_count > kMaxFsstSymbolCount) return false;

  uint8_t wire_histogram[8];
  std::memcpy(wire_histogram, buffer + 1, 8);

  int sum = 0;
  for (int i = 0; i < 8; ++i) sum += wire_histogram[i];
  if (sum != symbol_count) return false;

  static constexpr uint8_t kWireLayoutLengths[8] = {2, 3, 4, 5, 6, 7, 8, 1};

  int expected_data = 0;
  for (int i = 0; i < 8; ++i) expected_data += wire_histogram[i] * kWireLayoutLengths[i];
  if (len < kFsstSymbolTableHeaderSize + expected_data) return false;

  std::memset(decoder, 0, sizeof(fsst_decoder_t));
  decoder->version = 20190218ULL;
  decoder->zeroTerminated = 0;

  const uint8_t* data_ptr = buffer + kFsstSymbolTableHeaderSize;
  int code = 0;
  for (int i = 0; i < 8; ++i) {
    const uint8_t L = kWireLayoutLengths[i];
    for (int k = 0; k < wire_histogram[i]; ++k, ++code) {
      decoder->len[code] = L;
      uint64_t sym = 0;
      std::memcpy(&sym, data_ptr, L);
      decoder->symbol[code] = sym;
      data_ptr += L;
    }
  }

  return true;
}

}  
