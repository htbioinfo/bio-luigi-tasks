# -*- coding: utf-8 -*-
import gzip
from shutil import copyfileobj

from luigi import Task, PathParameter, LocalTarget


class MergeDir(Task):
    dir = PathParameter(exists=True)
    file = PathParameter(exists=False)

    def output(self):
        return LocalTarget(self.file)

    def run(self):
        with self.file.open('wb') as ofp:  # noqa
            for infile in self.dir.iterdir():  # noqa
                if not infile.is_file():
                    continue
                with infile.open('rb') as ifp:
                    copyfileobj(ifp, ofp)


class MergeCompressedFastqDir(Task):
    dir = PathParameter(exists=True)
    file = PathParameter(exists=False)

    exts = ['.fastq.gz', '.fq.gz']

    def output(self):
        return LocalTarget(self.file)

    def _is_fastq_gz(self, fn):
        for ext in self.exts:
            if fn.endswith(ext):
                return True
        return False

    def run(self):
        with self.file.open('wb') as ofp:  # noqa
            for infile in self.dir.iterdir():  # noqa
                if not self._is_fastq_gz(str(infile)):
                    continue
                with gzip.open(infile, 'rb') as ifp:
                    copyfileobj(ifp, ofp)
