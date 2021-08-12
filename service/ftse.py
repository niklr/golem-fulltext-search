#!/usr/bin/env python3
import argparse
import json
import os
import shutil
from pathlib import Path
from whoosh.fields import Schema, TEXT, NUMERIC, ID
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from whoosh.highlight import set_matched_filter
from whoosh.searching import Hit

IN_PATH = Path("/golem/in").absolute()
OUT_PATH = Path("/golem/out").absolute()

SCHEMA = Schema(
    content=TEXT(stored=True),
    line_number=NUMERIC(int, 32, stored=True, signed=False),
    path=ID(stored=True))


class FtseResult:
    def __init__(self, filename, line, positions):
        self.filename = filename
        self.lines = list()
        self.lines.append(FtseLineResult(
            line,
            positions
        ))


class FtseLineResult:
    def __init__(self, line, positions):
        self.line = line
        self.positions = positions


class FullTextSearchEngine:
    ix = None
    fieldname = "content"

    def init_index(self, in_path: str, out_path: str):
        index_path = self.get_index_path(out_path)
        if os.path.exists(index_path):
            shutil.rmtree(index_path)
        os.mkdir(index_path)
        self.ix = create_in(index_path, SCHEMA)
        writer = self.ix.writer()
        in_path = IN_PATH if in_path.isspace() == True else in_path
        for filename in os.listdir(in_path):
            with open(os.path.join(in_path, filename)) as f:
                for line_number, line in enumerate(f):
                    writer.add_document(
                        content=line,
                        line_number=line_number,
                        path=filename,
                    )
        writer.commit()

    def get_index_path(self, out_path: str):
        out_path = OUT_PATH if out_path.isspace() == True else out_path
        Path(out_path).mkdir(parents=True, exist_ok=True)
        index_path = os.path.join(out_path, "index")
        return index_path

    # Source: https://github.com/mchaput/whoosh/blob/main/src/whoosh/highlight.py
    def get_positions(self, hitobj: Hit):
        results = hitobj.results
        schema = results.searcher.schema
        field = schema[self.fieldname]
        from_bytes = field.from_bytes

        if not results.has_matched_terms():
            return

        # Get the terms searched for/matched in this field
        bterms = (term for term in results.matched_terms()
                  if term[0] == self.fieldname)

        # Convert bytes to unicode
        words = frozenset(from_bytes(term[1]) for term in bterms)
        analyzer = hitobj.searcher.schema[self.fieldname].analyzer
        tokens = analyzer(hitobj[self.fieldname], positions=True,
                          chars=True, mode="index", removestops=False)
        tokens = set_matched_filter(tokens, words)
        positions = list()
        for t in tokens:
            if t.matched == True:
                positions.append(t.startchar)
        return positions

    def read_file(self, path: str):
        f = open(path, "r")
        content = f.read()
        f.close()
        return content

    def obj_dict(self, obj):
        return obj.__dict__

    def search(self, term: str):
        with self.ix.searcher() as searcher:
            query = QueryParser(self.fieldname, self.ix.schema).parse(term)
            search_result = searcher.search(query, terms=True)
            results = dict()
            for result in search_result:
                positions = self.get_positions(result)
                filename = result['path']
                if filename in results:
                    results[filename].lines.append(
                        FtseLineResult(
                            result['line_number'],
                            positions
                        )
                    )
                else:
                    results[filename] = FtseResult(
                        result['path'],
                        result['line_number'],
                        positions)
            return json.dumps(list(results.values()), default=self.obj_dict)


ftse = FullTextSearchEngine()


def test():
    print("test")


def init():
    ftse.init_index(IN_PATH.as_posix(), OUT_PATH.as_posix())
    print("init success")


def search(term: str):
    result = ftse.search(term)
    print(result)


def dump(filename: str):
    content = ftse.read_file(os.path.join(IN_PATH, filename))
    print(content)


def get_arg_parser():
    parser = argparse.ArgumentParser(description="ftse service")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true")
    group.add_argument("--init", action="store_true")
    group.add_argument("--search", help="Provide a term to be searched for")
    group.add_argument("--dump", help="Provide a filename to be dumped")
    return parser


if __name__ == "__main__":
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()

    if args.test:
        test()
    elif args.init:
        init()
    elif args.search:
        search(args.search)
    elif args.dump:
        dump(args.dump)
