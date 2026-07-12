import os
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

DATA = os.path.expanduser(os.environ.get("DATA_DIR", "./data"))
OUT = os.path.expanduser(os.environ.get("OUT_DIR", os.path.join(DATA, "_preprocessed")))
os.makedirs(OUT, exist_ok=True)
TARGET_ROWS = int(os.environ.get("TARGET_ROWS", 10_000_000))
MAX_RAW_BYTES = int(os.environ.get("MAX_RAW_BYTES", 1_800_000_000))


def write_replicated(name, strings):
    if not strings:
        return
    arr = pa.array(strings, type=pa.string())
    src_rows = len(arr)
    src_raw = sum(len(s.encode() if isinstance(s, str) else s) for s in strings)
    target = min(TARGET_ROWS, int(MAX_RAW_BYTES / max(1.0, src_raw / max(1, src_rows))))
    schema = pa.schema([pa.field("col", pa.string())])
    written = 0
    with pq.ParquetWriter(os.path.join(OUT, f"{name}.parquet"), schema, compression=None,
                          use_dictionary=False, column_encoding="PLAIN",
                          data_page_version="2.0") as w:
        while written < target:
            remaining = target - written
            take = arr if remaining >= src_rows else arr.slice(0, remaining)
            w.write_batch(pa.record_batch([take], schema=schema))
            written += src_rows if remaining >= src_rows else remaining
    print(f"{name}: {written:,} rows")


def from_csv(name, path, col):
    opts = pcsv.ConvertOptions(column_types={col: pa.string()}, include_columns=[col])
    write_replicated(name, pcsv.read_csv(path, convert_options=opts)[col].combine_chunks().to_pylist())


def from_lines(name, path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        write_replicated(name, [ln.rstrip("\n") for ln in f])


from_csv("rag_qa_chunk_text", f"{DATA}/rag-qa/rag_corpus_chunks.csv", "chunk_text")
from_csv("ecom_accessed_date", f"{DATA}/ecom/E-commerce Website Logs.csv", "accessed_date")
from_csv("ecom_bytes", f"{DATA}/ecom/E-commerce Website Logs.csv", "bytes")
from_csv("tpch_ps_comment", f"{DATA}/tpch/ps_comment.csv", "ps_comment")
from_csv("cliwoc_otherrem", f"{DATA}/cliwoc/CLIWOC15.csv", "OtherRem")
from_lines("elastic_line", f"{DATA}/elastic_apache_logs.txt")
from_lines("mind_news_line", f"{DATA}/mind/news.tsv")
from_lines("server_logs_line", f"{DATA}/server-logs/logfiles_500k.log")
