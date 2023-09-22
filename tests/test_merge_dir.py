# -*- coding: utf-8 -*-
import tempfile
from os.path import join

from bio_luigi_tasks import MergeDir, MergeCompressedFastqDir


def test_merge_dir():
    dir = 'tests/data/test_merge_dir/texts'  # noqa
    with tempfile.TemporaryDirectory() as parent:
        file = join(parent, 'foobar')
        task = MergeDir(dir=dir, file=file)
        task.run()
        with open(file) as fp:
            line1, line2 = fp.readlines()
            assert (line1 == 'foo\n') or (line1 == 'bar\n')
            assert (line2 == 'foo\n') or (line2 == 'bar\n')


def test_merge_compressed_fastq_dir():
    dir = 'tests/data/test_merge_dir/compressed_fastq'  # noqa
    with tempfile.TemporaryDirectory() as parent:
        file = join(parent, 'foobar.fastq')
        task = MergeCompressedFastqDir(dir=dir, file=file)
        task.run()
        with open(file) as fp:
            line1, line2, line3, line4, line5, line6, line7, line8 = fp.readlines()
            assert (line1 == '@foo\n') or (line1 == '@bar\n')
            assert line2 == 'ATCG\n'
            assert line3 == '+\n'
            assert line4 == 'abcd\n'
            assert (line5 == '@foo\n') or (line5 == '@bar\n')
            assert line6 == 'ATCG\n'
            assert line7 == '+\n'
            assert line8 == 'abcd\n'
