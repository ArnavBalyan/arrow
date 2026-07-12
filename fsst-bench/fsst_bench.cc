#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::high_resolution_clock;

std::shared_ptr<arrow::StringArray> LoadLines(const std::string& path) {
    arrow::StringBuilder b;
    std::ifstream f(path);
    if (!f) { std::cerr << "cannot open " << path << "\n"; std::exit(1); }
    std::string line;
    while (std::getline(f, line)) {
        if (!b.Append(line).ok()) std::exit(1);
    }
    std::shared_ptr<arrow::Array> arr;
    if (!b.Finish(&arr).ok()) std::exit(1);
    return std::static_pointer_cast<arrow::StringArray>(arr);
}

std::shared_ptr<arrow::StringArray> LoadParquetColumn(const std::string& path) {
    auto input_result = arrow::io::ReadableFile::Open(path);
    if (!input_result.ok()) {
        std::cerr << "open " << path << " failed: " << input_result.status() << "\n";
        std::exit(1);
    }
    auto reader_result = parquet::arrow::OpenFile(
        *input_result, arrow::default_memory_pool());
    if (!reader_result.ok()) std::exit(1);
    auto reader = std::move(*reader_result);
    auto table_result = reader->ReadTable();
    if (!table_result.ok()) std::exit(1);
    auto table = *table_result;
    auto chunked = table->column(0);
    arrow::StringBuilder b;
    for (int ci = 0; ci < chunked->num_chunks(); ++ci) {
        auto chunk = std::static_pointer_cast<arrow::StringArray>(chunked->chunk(ci));
        for (int64_t i = 0; i < chunk->length(); ++i) {
            if (chunk->IsNull(i)) {
                if (!b.AppendNull().ok()) std::exit(1);
            } else {
                if (!b.Append(chunk->GetView(i)).ok()) std::exit(1);
            }
        }
    }
    std::shared_ptr<arrow::Array> out;
    if (!b.Finish(&out).ok()) std::exit(1);
    return std::static_pointer_cast<arrow::StringArray>(out);
}

struct EncConfig {
    const char* name;
    parquet::Encoding::type enc;
    bool use_dict;
    parquet::Compression::type comp;
};

static const std::vector<EncConfig> kConfigs = {
    {"Plain+ZSTD",    parquet::Encoding::PLAIN,             false, parquet::Compression::ZSTD},
    {"Dict+ZSTD",     parquet::Encoding::PLAIN,             true,  parquet::Compression::ZSTD},
    {"DeltaBA+ZSTD",  parquet::Encoding::DELTA_BYTE_ARRAY,  false, parquet::Compression::ZSTD},
    {"FSST+ZSTD",     parquet::Encoding::FSST,              false, parquet::Compression::ZSTD},
    {"Plain+LZ4",     parquet::Encoding::PLAIN,             false, parquet::Compression::LZ4_HADOOP},
    {"Dict+LZ4",      parquet::Encoding::PLAIN,             true,  parquet::Compression::LZ4_HADOOP},
    {"DeltaBA+LZ4",   parquet::Encoding::DELTA_BYTE_ARRAY,  false, parquet::Compression::LZ4_HADOOP},
    {"FSST+LZ4",      parquet::Encoding::FSST,              false, parquet::Compression::LZ4_HADOOP},
    {"FSST",          parquet::Encoding::FSST,              false, parquet::Compression::UNCOMPRESSED},
};

struct RunResult {
    int64_t file_bytes;
    double encode_ms;
    double decode_ms;
};

RunResult RunOne(const std::shared_ptr<arrow::StringArray>& col,
                 const EncConfig& cfg,
                 const std::string& temp_path,
                 int decode_iters) {
    auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
    auto table = arrow::Table::Make(schema, {std::make_shared<arrow::ChunkedArray>(col)});

    parquet::WriterProperties::Builder pb;
    pb.encoding(cfg.enc);
    pb.compression(cfg.comp);
    if (cfg.use_dict) pb.enable_dictionary(); else pb.disable_dictionary();
    if (cfg.comp == parquet::Compression::ZSTD) {
        pb.compression_level(3);
    }
    auto props = pb.build();

    parquet::ArrowWriterProperties::Builder apb;
    apb.store_schema();
    auto arrow_props = apb.build();

    auto sink_result = arrow::io::FileOutputStream::Open(temp_path);
    if (!sink_result.ok()) {
        std::cerr << "sink open failed: " << sink_result.status() << "\n"; std::exit(1);
    }
    auto sink = *sink_result;

    auto t0 = Clock::now();
    auto write_status = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), sink, table->num_rows(),
        props, arrow_props);
    if (!write_status.ok()) {
        std::cerr << "write failed [" << cfg.name << "]: " << write_status << "\n";
        return {-1, -1, -1};
    }
    if (!sink->Close().ok()) std::exit(1);
    auto t1 = Clock::now();
    double encode_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    int64_t file_bytes = fs::file_size(temp_path);

    std::vector<double> decode_times;
    decode_times.reserve(decode_iters);
    for (int iter = 0; iter < decode_iters; ++iter) {
        auto input_result = arrow::io::ReadableFile::Open(temp_path);
        if (!input_result.ok()) std::exit(1);
        auto td0 = Clock::now();
        auto reader_result = parquet::arrow::OpenFile(
            *input_result, arrow::default_memory_pool());
        if (!reader_result.ok()) std::exit(1);
        auto reader = std::move(*reader_result);
        auto table_result = reader->ReadTable();
        if (!table_result.ok()) std::exit(1);
        auto out_table = *table_result;
        int64_t touched = 0;
        for (int ci = 0; ci < out_table->column(0)->num_chunks(); ++ci) {
            auto ch = std::static_pointer_cast<arrow::StringArray>(
                out_table->column(0)->chunk(ci));
            touched += ch->total_values_length();
        }
        if (touched < 0) std::cerr << "huh\n";
        auto td1 = Clock::now();
        decode_times.push_back(std::chrono::duration<double, std::milli>(td1 - td0).count());
    }
    std::sort(decode_times.begin(), decode_times.end());
    double decode_ms = decode_times[decode_times.size() / 2];

    return {file_bytes, encode_ms, decode_ms};
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: fsst_bench <loader> <path> <label>\n"
                  << "  loader: lines | parquet\n";
        return 1;
    }
    std::string loader = argv[1];
    std::string path = argv[2];
    std::string label = argv[3];

    std::shared_ptr<arrow::StringArray> col;
    if (loader == "lines") col = LoadLines(path);
    else if (loader == "parquet") col = LoadParquetColumn(path);
    else { std::cerr << "unknown loader: " << loader << "\n"; return 1; }

    int64_t raw_bytes = 0;
    for (int64_t i = 0; i < col->length(); ++i) {
        if (!col->IsNull(i)) raw_bytes += col->GetView(i).size();
    }
    std::cerr << "loaded " << label << ": " << col->length() << " rows, "
              << raw_bytes << " raw bytes\n";

    std::cout << "dataset,rows,raw_bytes,config,file_bytes,encode_ms,decode_ms\n";

    std::string temp = "/tmp/fsst_bench_tmp.parquet";
    int decode_iters = (col->length() > 200000) ? 3 : 7;

    for (const auto& cfg : kConfigs) {
        std::cerr << "  " << cfg.name << " ..." << std::flush;
        auto r = RunOne(col, cfg, temp, decode_iters);
        std::cerr << " file=" << r.file_bytes << "B  enc=" << r.encode_ms
                  << "ms dec=" << r.decode_ms << "ms\n";
        std::printf("%s,%lld,%lld,%s,%lld,%.2f,%.2f\n",
                    label.c_str(),
                    (long long)col->length(), (long long)raw_bytes, cfg.name,
                    (long long)r.file_bytes, r.encode_ms, r.decode_ms);
    }
    return 0;
}
