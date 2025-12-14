import os
import sys

# English comments only:
# Validates that the provided data directory exists and is readable.

def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: validate_inputs.py <data_dir>")
        return 2

    data_dir = sys.argv[1]
    if not os.path.isdir(data_dir):
        print(f"ERROR: Not a directory: {data_dir}")
        return 1

    if not os.access(data_dir, os.R_OK):
        print(f"ERROR: Directory not readable: {data_dir}")
        return 1

    print("OK")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
