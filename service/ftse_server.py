#!/usr/bin/env python3
import json
import os
import shutil
from pathlib import Path
from whoosh.fields import Schema, TEXT, NUMERIC, ID
from whoosh.index import FileIndex, create_in
from whoosh.qparser import QueryParser
from whoosh.highlight import set_matched_filter
from whoosh.searching import Hit
import rpyc
from rpyc.utils.server import ThreadedServer
from threading import Thread

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


class FullTextSearchEngine(rpyc.Service):
    def exposed_init_index(self, in_path: str, out_path: str):
        server.ix = self.init_index(in_path, out_path)
        return True

    def init_index(self, in_path: str, out_path: str):
        index_path = self.get_index_path(out_path)
        if os.path.exists(index_path):
            shutil.rmtree(index_path)
        os.mkdir(index_path)
        ix = create_in(index_path, SCHEMA)
        writer = ix.writer()
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
        return ix

    def get_index_path(self, out_path: str):
        out_path = OUT_PATH if out_path.isspace() == True else out_path
        Path(out_path).mkdir(parents=True, exist_ok=True)
        index_path = os.path.join(out_path, "index")
        return index_path

    # Source: https://github.com/mchaput/whoosh/blob/main/src/whoosh/highlight.py
    def exposed_get_positions(self, hitobj: Hit, fieldname: str):
        results = hitobj.results
        schema = results.searcher.schema
        field = schema[fieldname]
        from_bytes = field.from_bytes

        if not results.has_matched_terms():
            return

        # Get the terms searched for/matched in this field
        bterms = (term for term in results.matched_terms()
                  if term[0] == fieldname)

        # Convert bytes to unicode
        words = frozenset(from_bytes(term[1]) for term in bterms)
        analyzer = hitobj.searcher.schema[fieldname].analyzer
        tokens = analyzer(hitobj[fieldname], positions=True,
                          chars=True, mode="index", removestops=False)
        tokens = set_matched_filter(tokens, words)
        positions = list()
        for t in tokens:
            if t.matched == True:
                positions.append(t.startchar)
        return positions

    def exposed_read_file(self, path: str):
        f = open(path, "r")
        content = f.read()
        f.close()
        return content

    def obj_dict(self, obj):
        return obj.__dict__

    def exposed_search(self, term: str):
        ix = server.ix
        return self.search(ix, term)

    def search(self, ix: FileIndex, term: str):
        with ix.searcher() as searcher:
            fieldname = "content"
            query = QueryParser(fieldname, ix.schema).parse(term)
            search_result = searcher.search(query, terms=True)
            results = dict()
            for result in search_result:
                positions = self.exposed_get_positions(result, fieldname)
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


if __name__ == "__main__":
    print("Server starting")
    server = ThreadedServer(
        FullTextSearchEngine,
        socket_path='/golem/run/uds_socket')
    t = Thread(target=server.start)
    t.daemon = True
    t.start()
    t.join()
    print("Server started")