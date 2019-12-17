"""Luigi targets for dask collections"""

from dask import delayed
from dask.bytes.core import get_fs_token_paths
from dask.dataframe import read_csv, read_parquet
from luigi import Target
from luigi.task import logger as luigi_logger
from luigi.local_target import LocalTarget, atomic_file
import random
from pathlib import Path
from contextlib import contextmanager
import os


FLAG = "_SUCCESS"  # Task success flag, c.f. Spark, hadoop, etc


@delayed(pure=True)
def touch(path, storage_options=None, _dep=None):
    fs, token, paths = get_fs_token_paths(path, storage_options=storage_options)
    with fs.open(path, mode="wb"):
        pass







class suffix_preserving_atomic_file(atomic_file):
    def generate_tmp_path(self, path):
        # get suffix (allows for more than one, e.g. .tar.gz) and pass it to tempfile.mkstemp
        base_path, file_name = os.path.split(path)
        return os.path.join(base_path, "luigi-tmp-{0}-{1}".format(random.randrange(0, 1e10), file_name))


class BaseAtomicProviderLocalTarget(LocalTarget):
    # Allow some composability of atomic handling
    atomic_provider = atomic_file

    def open(self, mode='r'):
        # leverage super() as well as modifying any code in LocalTarget
        # to use self.atomic_provider rather than atomic_file

        rwmode = mode.replace('b', '').replace('t', '')
        if rwmode == 'w':
            self.makedirs()
            return self.format.pipe_writer(self.atomic_provider(self.path))

        return super().open(mode=mode)

    @contextmanager
    def temporary_path(self):
        # NB: unclear why LocalTarget doesn't use atomic_file in its implementation
        self.makedirs()
        with self.atomic_provider(self.path) as af:
            yield af.tmp_path


class SuffixPreservingLocalTarget(BaseAtomicProviderLocalTarget):
    atomic_provider = suffix_preserving_atomic_file
