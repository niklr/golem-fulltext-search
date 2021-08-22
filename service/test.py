#!/usr/bin/env python3
from ftse_server import FullTextSearchEngine
from pathlib import Path

DATA_PATH = Path(__file__).absolute().parent / "data"
OUT_PATH = Path(__file__).absolute().parent / "out"

def main():
    ftse = FullTextSearchEngine()
    ix = ftse.init_index(DATA_PATH.as_posix(), OUT_PATH.as_posix())
    result1 = ftse.search(ix, "golem")
    print(f"Result 1: {result1}")
    result2 = ftse.search(ix, "network")
    print(f"Result 2: {result2}")

if __name__ == "__main__":
    main()
