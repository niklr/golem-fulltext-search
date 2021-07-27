#!/usr/bin/env python3
from ftse import FullTextSearchEngine
from pathlib import Path

DATA_PATH = Path(__file__).absolute().parent / "data"
OUT_PATH = Path(__file__).absolute().parent / "out"

def main():
    ftse = FullTextSearchEngine()
    ftse.init_index(DATA_PATH.as_posix(), OUT_PATH.as_posix())
    result1 = ftse.search(OUT_PATH.as_posix(), "golem")
    print(f"Result 1: {result1}")

if __name__ == "__main__":
    main()
