import argparse, glob, os, re, struct
from typing import Optional, Tuple

MAGIC = b"PAR1"

FLAG_PATTERNS = [
    re.compile(rb"FLAG\{[^}]{1,200}\}"),
    re.compile(rb"flag\{[^}]{1,200}\}"),
    re.compile(rb"CTF\{[^}]{1,200}\}"),
]

def read_tail(path: str, max_bytes: int = 8_000_000) -> bytes:
    sz = os.path.getsize(path)
    n = min(sz, max_bytes)
    with open(path, "rb") as f:
        f.seek(sz - n)
        return f.read(n)

def find_flag_in_bytes(b: bytes) -> Optional[bytes]:
    for pat in FLAG_PATTERNS:
        m = pat.search(b)
        if m:
            return m.group(0)
    return None

def plausible_footer(sz: int, footer_len: int) -> bool:
    # Ensure footer_len is reasonable and doesn't exceed file boundaries
    if footer_len <= 0:
        return False
    if footer_len > 100_000_000:  # Footer larger than 100MB is suspicious
        return False
    footer_start = sz - 8 - footer_len
    return footer_start >= 0

def try_repair_by_last_magic(path: str) -> Tuple[bool, Optional[bytes], Optional[int]]:
    """
    Strategy:
    1) Find the last PAR1 magic in the file's tail section
    2) Read the footer_len before it
    3) If footer_len is plausible, "real end" = the end of that PAR1
       and return repaired bytes by truncating the file to that length
    """
    sz = os.path.getsize(path)
    tail = read_tail(path)
    tail_off = sz - len(tail)

    idx = tail.rfind(MAGIC)
    if idx == -1:
        return False, None, None

    real_end = tail_off + idx + 4  # PAR1-in sonu
    real_end = tail_off + idx + 4  # End of the last PAR1 magic
    if real_end < 8:
        return False, None, None

    # footer_len = 4 byte (real_end - 8 .. real_end - 4)
    with open(path, "rb") as f:
        f.seek(real_end - 8)
        footer_len_bytes = f.read(4)
        if len(footer_len_bytes) != 4:
            return False, None, None
        footer_len = struct.unpack("<I", footer_len_bytes)[0]

    if not plausible_footer(real_end, footer_len):
        return False, None, real_end

    with open(path, "rb") as f:
        f.seek(0)
        repaired = f.read(real_end)

    return True, repaired, real_end

def repair_and_write(in_path: str, out_path: str) -> dict:
    # 1) əvvəl tail-də flag axtar (garbage + footer region)
    # 1) Search for the flag in the tail (garbage + footer region)
    tail = read_tail(in_path)
    flag = find_flag_in_bytes(tail)
    flag_str = flag.decode("utf-8", errors="replace") if flag else None

    # 2) repair try
    ok, repaired_bytes, real_end = try_repair_by_last_magic(in_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if ok and repaired_bytes:
        with open(out_path, "wb") as f:
            f.write(repaired_bytes)
        status = "repaired_by_truncation"
    else:
        # if nothing works, copy the original (maybe the file is already healthy)
        with open(in_path, "rb") as src, open(out_path, "wb") as dst:
            dst.write(src.read())
        status = "copied_unmodified"

    return {"input": in_path, "output": out_path, "status": status, "flag": flag_str, "real_end": real_end}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--mode", required=True, choices=["flag", "repair"])
    args = ap.parse_args()

    files = glob.glob(os.path.join(args.data_dir, "**", "*.parquet"), recursive=True)
    if not files:
        raise SystemExit("No parquet files found.")

    if args.mode == "flag":
        found = []
        for fp in sorted(files):
            tail = read_tail(fp)
            flag = find_flag_in_bytes(tail)
            if flag:
                found.append((fp, flag.decode("utf-8", errors="replace")))
        if not found:
            print("NO_FLAG_FOUND")
        else:
            for fp, fl in found:
                print(f"{os.path.basename(fp)}: {fl}")

    else:  # repair
        out_dir = "processed_data"
        reports = []
        for fp in sorted(files):
            out_path = os.path.join(out_dir, os.path.basename(fp))
            rep = repair_and_write(fp, out_path)
            reports.append(rep)

        repaired = sum(1 for r in reports if r["status"] == "repaired_by_truncation")
        print(f"Processed {len(reports)} parquet files. Repaired={repaired}. Output=processed_data/")

if __name__ == "__main__":
    main()
