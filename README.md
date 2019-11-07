<<<<<<< HEAD
# Pset 1
<!-- Adding Travis Climate Things here
-->
[![Build Status](https://travis-ci.com/csci-e-29/2019fa-pset-1-naarla.svg?token=jrhzVcBhxR2g4ZmHNfrx&branch=master)](https://travis-ci.com/csci-e-29/2019fa-pset-1-naarla)

[![Maintainability](https://api.codeclimate.com/v1/badges/b84ec09f03bbd90a2bae/maintainability)](https://codeclimate.com/repos/5d8fe3943193dc0162000010/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/b84ec09f03bbd90a2bae/test_coverage)](https://codeclimate.com/repos/5d8fe3943193dc0162000010/test_coverage)


<!-- START doctoc generated TOC please keep comment here to allow auto update -->

<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Before you begin...](#before-you-begin)
  - [Document code and read documentation](#document-code-and-read-documentation)
  - [Docker shortcut](#docker-shortcut)
  - [Pipenv](#pipenv)
    - [Installation](#installation)
    - [Usage](#usage)
      - [Pipenv inside docker](#pipenv-inside-docker)
  - [Credentials and data](#credentials-and-data)
    - [Using `awscli`](#using-awscli)
      - [Installation (via pipenv)](#installation-via-pipenv)
    - [Configure `awscli`](#configure-awscli)
      - [Make a .env (say: dotenv) file](#make-a-env-say-dotenv-file)
      - [Other methods](#other-methods)
    - [Copy the data locally](#copy-the-data-locally)
    - [Set the Travis environment variables](#set-the-travis-environment-variables)
- [Problems (40 points)](#problems-40-points)
  - [Hashed strings (10 points)](#hashed-strings-10-points)
    - [Implement a standardized string hash (5 points)](#implement-a-standardized-string-hash-5-points)
    - [True salting (5 points)](#true-salting-5-points)
  - [Atomic writes (15 points)](#atomic-writes-15-points)
    - [Implement an atomic write (15 points)](#implement-an-atomic-write-15-points)
  - [Parquet (15 points)](#parquet-15-points)
  - [Your main script](#your-main-script)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Before you begin...

**Add your Travis and Code Climate badges** to the
top of this README, using the markdown template for your master branch.

### Document code and read documentation

For some problems we have provided starter code. Please look carefully at the
doc strings and follow all input and output specifications.

For other problems, we might ask you to create new functions, please document
them using doc strings! Documentation factors into the "python quality" portion
of your grade.

### Docker shortcut

See [drun_app](./drun_app):

```bash
docker-compose build
./drun_app python # Equivalent to docker-compose run app python
```

### Pipenv

This pset will require dependencies.  Rather than using a requirements.txt, we
will use [pipenv](https://pipenv.readthedocs.io/en/latest/) to give us a pure,
repeatable, application environment.

#### Installation

If you are using the Docker environment, you should be good to go.  Mac/windows
users should [install
pipenv](https://pipenv.readthedocs.io/en/latest/#install-pipenv-today) into
their main python environment as instructed.  If you need a new python 3.7
environment, you can use a base
[conda](https://docs.conda.io/en/latest/miniconda.html) installation.

```bash
# Optionally create a new base python 3.7
conda create -n py37 python=3.7
conda activate py37
pip install pipenv
pipenv install ...
```

```bash
pipenv install --dev
pipenv run python some_python_file
```

If you get a TypeError, see [this
issue](https://github.com/pypa/pipenv/issues/3363)

#### Usage

Rather than `python some_file.py`, you should run `pipenv run python some_file.py`
or `pipenv shell` etc

***Never*** pip install something!  Instead you should `pipenv install
pandas` or `pipenv install --dev ipython`.  Use `--dev` if your app
only needs the dependency for development, not to actually do it's job.

Pycharm [works great with
pipenv](https://www.jetbrains.com/help/pycharm/pipenv.html)

Be sure to commit any changes to your [Pipfile](./Pipfile) and
[Pipfile.lock](./Pipfile.lock)!

##### Pipenv inside docker

Because of the way docker freezes the operating system, installing a new package
within docker is a two-step process:

```bash
docker-compose build

# Now i want a new thing
./drun_app pipenv install pandas # Updates pipfile, but does not rebuild image
# running ./drun_app python -c "import pandas" will fail!

# Rebuild
docker-compose build
./drun_app python -c "import pandas" # Now this works
```

### Credentials and data

Git should be for code, not data, so we've created an S3 bucket for problem set
file distribution.  For this problem set, we've uploaded a data set of your
answers to the "experience demographics" quiz that you should have completed in
the first week. In order to access the data in S3, we need to install and
configure `awscli` both for running the code locally and running our tests in
Travis.

You should have created an IAM key in your AWS account.  DO NOT SHARE THE SECRET
KEY WITH ANYONE. It gives anyone access to the S3 bucket.  It must not be
committed to your code.

For more reference on security, see [Travis Best
Practices](https://docs.travis-ci.com/user/best-practices-security/#recommendations-on-how-to-avoid-leaking-secrets-to-build-logs)
and [Removing Sensitive
Data](https://help.github.com/articles/removing-sensitive-data-from-a-repository/).

#### Using `awscli`

AWS provides a [CLI
tool](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
that helps interact with the many different services they offer.

##### Installation (via pipenv)

We have already installed `awscli` into your pipenv.  It is available within
the pipenv shell and the docker container via the same mechanism.

```bash
pipenv run aws --help
./drun_app aws --help
```

#### Configure `awscli`

Now configure `awscli` to access the S3 bucket.

##### Make a .env (say: dotenv) file

Create a [.env](.env) file that looks something like this:

```
AWS_ACCESS_KEY_ID=XXXX
OTHER_ENV_VARIABLE=XXX
```

***DO NOT*** commit this file to the repo (it's already in your
[.gitignore](.gitignore))

Both docker and pipenv will automatically inject these variables into your
environment!  Whenever you need new env variables, add them to a dotenv.

See env refs for
[docker](https://docs.docker.com/compose/environment-variables/) and
[pipenv](https://pipenv.readthedocs.io/en/latest/advanced/#automatic-loading-of-env)
for more details.

##### Other methods

According to the
[documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
the easiest way is:

```bash
aws configure
AWS Access Key ID [None]: ACCESS_KEY
AWS Secret Access Key [None]: SECRET_KEY
Default region name [None]:
Default output format [None]:
```

There are other, more complicated, configurations outlined in the documentation.
Feel free to use a solution using environment variables, a credentials
file, a profile, etc.

#### Copy the data locally

Read the [Makefile](Makefile) and [.travis.yml](./.travis.yml) to see how to
copy the data locally.

Note that we are using a [Requestor
Pays](https://docs.aws.amazon.com/AmazonS3/latest/dev/RequesterPaysBuckets.html)
bucket.  You are responsible for usage charges.

You should now have a new folder called `data` in your root directory with the
data we'll use for this problem set. You can find more details breaking down
this command at the [S3 Documentation
Site](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html).

#### Set the Travis environment variables

This is Advanced Data Science, so of course we also want to automate our tests
and pset using CI/CD.  Unfortunately, we can't upload our .env or run  `aws
configure` and interactively enter the credentials for the Travis builds, so we
have to configure Travis to use the access credentials without compromising the
credentials in our git repo.

We've provided a working `.travis.yml` configuration that only requires the AWS
credentials when running on the master branch, but you will still need to the
final step of adding the variables for your specific pset repository.

To add environment variables to your Travis environment, you can use of the
following options:

* Navigating to the settings, eg https://travis-ci.com/csci-e-29/YOUR_PSET_REPO/settings
* The [Travis CLI](https://github.com/travis-ci/travis.rb)
* encrypting into the `.travis.yml` as instructed [here](https://docs.travis-ci.com/user/environment-variables/#defining-encrypted-variables-in-travisyml).

Preferably, you should only make your 'prod' credentials available on your
master branch: [Travis
Settings](https://docs.travis-ci.com/user/environment-variables/#defining-variables-in-repository-settings)

You can chose the method you think is most appropriate.  Our only requirement is
that ***THE KEYS SHOULD NOT BE COMMITTED TO YOUR REPO IN PLAIN TEXT ANYWHERE***.

For more information, check out the [Travis Documentation on Environment
Variables](https://docs.travis-ci.com/user/environment-variables/)

__*IMPORTANT*__: If you find yourself getting stuck or running into issues,
please post on Piazza and ask for help.  We've provided most of the instructions
necessary for this step and do not want you spinning your wheels too long just
trying to download the data.

## Problems (40 points)

### Hashed strings (10 points)

It can be extremely useful to ***hash*** a string or other data for various
reasons - to distribute/partition it, to anonymize it, or otherwise conceal the
content.

#### Implement a standardized string hash (5 points)

Use `sha256` as the backbone algorithm from
[hashlib](https://docs.python.org/3/library/hashlib.html).

A `salt` is a prefix that may be added to increase the randomness or otherwise
change the outcome.  It may be a `str` or `bytes` string, or empty.

Implement it in [hash_str.py](pset_1/hash_str.py), where the return value is the
`.digest()` of the hash, as a `bytes` array:

```python
def hash_str(some_val, salt=''):
    """Converts strings to hash digest

    :param str:
    :param str or bytes salt: string or bytes to add randomness to the hashing, defaults to ''

    :rtype: bytes
    """
```

Note you will need to `.encode()` string values into bytes.

As an example, `hash_str('world!', salt='hello, ').hex()[:6] == '68e656'`

Note that if we ever ask you for a bytes value in Canvas, the expectation is
the hexadecimal representation as illustrated above.

#### True salting (5 points)

Note, however, that hashing isn't very secure without a secure salt.  We
can take raw `bytes` to get something with more entropy than standard text
provides.

Let's designate an environment variable, `CSCI_SALT`, which will contain
hex-encoded bytes.  Implement the function `pset_utils.hash_str.get_csci_salt`
which pulls and decodes an environment variable.  In Canvas, you will be given
a random salt taken from [random.org](http://random.org) for real security.

### Atomic writes (15 points)

Use the module `pset_1.io`.  We will implement an atomic writer.

Atomic writes are used to ensure we never have an incomplete file output.
Basically, they perform the operations:

1. Create a temporary file which is unique (possibly involving a random file
   name)
2. Allow the code to take its sweet time writing to the file
3. Rename the file to the target destination name.

If the target and temporary file are on the same filesystem, the rename
operation is ***atomic*** - that is, it can only completely succeed or fail
entirely, and you can never be left with a bad file state.

See notes in
[Luigi](https://luigi.readthedocs.io/en/stable/luigi_patterns.html#atomic-writes-problem)
and the [Thanksgiving
Bug](https://www.arashrouhani.com/luigi-budapest-bi-oct-2015/#/21)

#### Implement an atomic write (15 points)

Start with the following in [io.py](./pset_1/io.py):

```python
@contextmanager
def atomic_write(file, mode='w', as_file=True, **kwargs):
    """Write a file atomically

    :param file: str or :class:`os.PathLike` target to write
    :param bool as_file:  if True, the yielded object is a :class:File.
        Otherwise, it will be the temporary file path string
    :param kwargs: anything else needed to open the file

    :raises: FileExistsError if target exists

    Example::

        with atomic_write("hello.txt") as f:
            f.write("world!")

    """
    ...
```

 Key considerations:

 * You can use [tempfile](https://docs.python.org/3.6/library/tempfile.html),
   write to the same directory as the target, or both.
   What are the tradeoffs? Add code comments for anything critical
 * Ensure the file is deleted if the writing code fails
 * Ensure the temporary file has the same extension(s) as the target.  This is
   important for any code that may infer something from the path (eg, `.tar.gz`)
 * If the writing code fails and you try again, the temp file should be new -
   you don't want the context to reopen the same temp file.
 * Others?

 Ensure these considerations are reflected in your unit tests!

***Every file written in this class must be written atomically, via this
function or otherwise.***

### Parquet (15 points)

Excel is a very poor file format compared to modern column stores.  Use [Parquet
via
Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#parquet)
to transform the provided excel file into a better format.

The new file should keep the same name, but use the extension `.parquet`.

Ensure you use your atomic write.

Read back ***just the hashed id column*** and print it (don't read the entire
data set!).

### Your main script

Implement top level execution in [pset_1/\__main__.py](pset_1/__main__.py) to
show your work and answer the q's in the pset answers quiz.  It can be
invoked with `python -m pset_1`.
=======
# Pset 5

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Before you begin](#before-you-begin)
  - [Salted](#salted)
  - [Dask](#dask)
    - [Requester pays, and a potential bug!](#requester-pays-and-a-potential-bug)
  - [Luigi warnings](#luigi-warnings)
  - [Testing Tasks](#testing-tasks)
  - [Pytest path](#pytest-path)
- [Problems](#problems)
  - [Composition](#composition)
    - [Requires and Requirement](#requires-and-requirement)
    - [TargetOutput](#targetoutput)
      - [Salted (Optional)](#salted-optional)
  - [Dask Targets](#dask-targets)
  - [Dask Analysis](#dask-analysis)
    - [Yelp Reviews](#yelp-reviews)
    - [Clean the data](#clean-the-data)
    - [Analysis](#analysis)
      - [CLI](#cli)
      - [ByDecade](#bydecade)
      - [ByStars](#bystars)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Before you begin

Begin with your cookiecutter template as usual, and manaully merge and link
to this repo as origin.

### Salted

This pset will (optionally) draw upon the Salted Graph workflow outlined and
demo'ed here:
[https://github.com/gorlins/salted](https://github.com/gorlins/salted)

You may use the code as reference, but it is not intended to be directly
imported - you should reimplement what you need in this repo or your csci_utils.

### Dask
You will likely need to `pipenv install dask[dataframe] fastparquet s3fs`

Ensure you are using `fastparquet` rather than `pyarrow` when working with dask.
This will work automatically so long as it is installed.

In the past, there have been issues installing fastparquet without including
`pytest-runner` in pipenv dev dependencies.

#### Requester pays, and a potential bug!

Dask targets must be configured to work with the s3 'requester pays' mode:

```python
target = SomeDaskTarget(storage_options=dict(requester_pays=True))
```

However, testing this pset with `s3fs==0.3.5`, it appears there is a bug that will
prevent dask from passing this config through!

In my testing, listing s3 objects (and calling `target.exists()`) works out of
the box, but actually reading the data failed with a PermissionDenied error.

Here is one possible monkey patch:

```python
from s3fs.core import S3File, _fetch_range as _backend_fetch

# A bug! _fetch_range does not pass through request kwargs
def _fetch_range(self, start, end):
    # Original _fetch_range does not pass req_kw through!
    return _backend_fetch(self.fs.s3, self.bucket, self.key, self.version_id, start, end, req_kw=self.fs.req_kw)
S3File._fetch_range = _fetch_range
```

(it would be great if someone confirms the bug, finds the best fix, and submits
a PR to s3fs!)

You may want to copy files locally (or even push them up to your own s3 bucket)
to test out your code before messing around with monkey patching s3fs.  If you
can't get this working, it is fine to copy down the csvs and push to your own
(private) s3 bucket for this problem set.

### Luigi warnings

Calling `luigi.build()` inside a test can raise some verbose and nasty warnings,
which are not useful.  While not the best practice, you can quickly silence
these by adding the following to the pytest config file:

```
filterwarnings =
    ignore::DeprecationWarning
    ignore::UserWarning
```

### Testing Tasks

Testing Luigi tasks end to end can be hard, but not impossible. You can write
tests using fake tasks:

```python
from tempfile import TemporaryDirectory
from luigi import Task, build

class SaltedTests(TestCase):
    def test_salted_tasks(self):
        with TemporaryDirectory() as tmp:
            class SomeTask(Task):
                output = SaltedOutput(file_pattern=os.path.join([tmp, '...']))
                ...

            # Decide how to test a salted workflow
```

Note that you can use `build([SomeTask()], local_scheduler=True)` inside a test
to fully run a luigi workflow, but you may want to suppress some warnings if you
do so.

### Pytest path

If you write any tests at the top level `tests/` directory, placing an empty
pytest config file named `conftest.py` at the top of the repository helps to
ensure pytest will set the path correctly at all times. This should not be
necessary if your tests are all inside your package.

## Problems

While you should be able to complete these tasks in order, if you get stuck, it
is possible to finish the dask computation without composing the task or even
doing a proper Luigi target. Ensure to capture your work in appropriate branches
to allow yourself to get unstuck if necessary.

### Composition

Review the descriptor patterns to replace `def output(self):` and  `def
requires(self):` with composition.

Create a new package/module `csci_utils.luigi.task` to capture this work.

#### Requires and Requirement

Finish the implementation of `Requires` and `Requirement` as discussed in
lecture.

```python
# csci_utils.luigi.task
class Requirement:
    def __init__(self, task_class, **params):
        ...

    def __get__(self, task, cls):
        return task.clone(
            self.task_class,
            **self.params)


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires`

    Example::

        class MyTask(Task):
            # Replace task.requires()
            requires = Requires()  
            other = Requirement(OtherTask)

            def run(self):
                # Convenient access here...
                with self.other.output().open('r') as f:
                    ...

        >>> MyTask().requires()
        {'other': OtherTask()}

    """

    def __get__(self, task, cls):
        if task is None:
            return self

        # Bind self/task in a closure
        return lambda : self(task)

    def __call__(self, task):
        """Returns the requirements of a task

        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.

        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        # Search task.__class__ for Requirement instances
        # return
        ...


class Requirement:
    def __init__(self, task_class, **params):
        ...

    def __get__(self, task, cls):
        if task is None:
            return self

        return task.clone(
            self.task_class,
            **self.params)
```

Tips:

* You can access all the properties of an object using `obj.__dict__` or
`dir(obj)`.  The latter will include inherited properties, while the former will
not.

* You can access the class of an object using `obj.__class__` or `type(obj)`.
  * eg, `MyTask().__class__ is MyTask`

* The properties on an instance are not necessarily the same as the properties
on a class, eg `other` is a `Requirement` instance when accessed from the class
`MyTask`, but should be an instance of `OtherTask` when accessed from an
instance of `MyTask`.

* You can get an arbitrary property of an object using `getattr(obj, 'asdf')`,
eg `{k: getattr(obj, k) for k in dir(obj)}`

* You can check the type using `isinstance(obj, Requirement)`

* You can use dict comprehensions eg `{k: v for k, v in otherdict.items() if
condition}`


#### TargetOutput

Implement a descriptor that can be used to generate a luigi target using
composition, inside the `task` module:

```python
# csci_utils.luigi.task
class TargetOutput:
    def __init__(self, file_pattern='{task.__class__.__name__}',
        ext='.txt', target_class=LocalTarget, **target_kwargs):
        ...

    def __get__(self, task, cls):
        if task is None:
            return self
        return lambda: self(task)

    def __call__(self, task):
        # Determine the path etc here
        ...
        return self.target_class(...)
```

Note the string patterns in file_pattern, which are intended to be used like
such:

```python
>>> '{task.param}-{var}.txt'.format(task=task, var='world')
'hello-world.txt'
```

See [str.format](https://docs.python.org/3.4/library/stdtypes.html#str.format)
for reference.

##### Salted (Optional)

This part is ***optional***.  You do not need to implement salted to receive
full credit, but you should play with it when you have time.

If you do not implement salted, you must still ensure your local answers are
correct before submitting them to canvas!

You can either create a subclass of `TargetOutput` or allow for this
functionality in the main class, eg:

```python
class SaltedOutput(TargetOutput):
    def __init__(self, file_pattern='{task.__class__.__name__}-{salt}', ...):
        ...
```

If the format string asks for the keyword `salt`, you should calculate the
task's salted id as a hexdigest and include it as a kwarg in the string format.

Refer to `get_salted_id` in the demo repo to get the globally unique salted data
id.

### Dask Targets

Dask outputs are typically folders; as such, they are not suitable for
directly using the luigi `FileSystemTarget` variants.  Dask uses its own file
system abstractions which are not compatible with Luigi's.

Correctly implementing the appropriate logic is a bit difficult, so you can
start with the included code.  Add it to `csci_utils.luigi.dask.target`:

```python
"""Luigi targets for dask collections"""

from dask import delayed
from dask.bytes.core import get_fs_token_paths
from dask.dataframe import read_csv, read_parquet, to_csv, to_parquet
from luigi import Target
from luigi.task import logger as luigi_logger

FLAG = "_SUCCESS"  # Task success flag, c.f. Spark, hadoop, etc


@delayed(pure=True)
def touch(path, storage_options=None, _dep=None):
    fs, token, paths = get_fs_token_paths(path, storage_options=storage_options)
    with fs.open(path, mode="wb"):
        pass


class BaseDaskTarget(Target):
    """Base target for dask collections

    The class provides reading and writing mechanisms to any filesystem
    supported by dask, as well as a standardized method of flagging a task as
    successfully completed (or checking it).

    These targets can be used even if Dask is not desired, as a way of handling
    paths and success flags, since the file structure (parquet/csvs/jsons etc)
    are identical to other Hadoop and alike systems.
    """

    def __init__(self, path, glob=None, flag=FLAG, storage_options=None):
        """

        :param str path: Directory the collection is stored in.  May be remote
            with the appropriate dask backend (ie s3fs)

        :param str flag: file under directory which indicates a successful
            completion, like a hadoop '_SUCCESS' flag.  If no flag is written,
            or another file cannot be used as a proxy, then the task can only
            assume a successful completion based on a file glob being
            representative, ie the directory exists or at least 1 file matching
            the glob. You must set the flag to '' or None to allow this fallback behavior.

        :param str glob: optional glob for files when reading only (or as a
            proxy for completeness) or to set the name for files to write for
            output types which require such.

        :param dict storage_options: used to create dask filesystem or passed on read/write
        """

        self.glob = glob
        self.path = path
        self.storage_options = storage_options or {}
        self.flag = flag

        if not path.endswith(self._get_sep()):
            raise ValueError("Must be a directory!")

    @property
    def fs(self):
        fs, token, paths = get_fs_token_paths(
            self.path, storage_options=self.storage_options
        )
        return fs

    def _get_sep(self):
        """The path seperator for the fs"""
        try:
            return self.fs.sep  # Set for local files etc
        except AttributeError:
            # Typical for s3, hdfs, etc
            return "/"

    def _join(self, *paths):
        sep = self._get_sep()
        pths = [p.rstrip(sep) for p in paths]
        return sep.join(pths)

    def _exists(self, path):
        """Check if some path or glob exists

        This should not be an implementation, yet the underlying FileSystem
        objects do not share an interface!

        :rtype: bool
        """

        try:
            _e = self.fs.exists
        except AttributeError:
            try:
                return len(self.fs.glob(path)) > 0
            except FileNotFoundError:
                return False
        return _e(path)

    def exists(self):
        # NB: w/o a flag, we cannot tell if a task is partially done or complete
        fs = self.fs
        if self.flag:
            ff = self._join(self.path, self.flag)
            if hasattr(fs, 'exists'):
                # Unfortunately, not every dask fs implemenents this!
                return fs.exists(ff)

        else:
            ff = self._join(self.path, self.glob or "")

        try:
            for _ in fs.glob(ff):
                # If a single file is found, assume exists
                # for loop in case glob is iterator, no need to consume all
                return True

            # Empty list?
            return False

        except FileNotFoundError:
            return False

    def mark_complete(self, _dep=None, compute=True):
        if not self.flag:
            raise RuntimeError("No flag for task, cannot mark complete")

        flagfile = self._join(self.path, self.flag)

        out = touch(flagfile, _dep=_dep)
        if compute:
            out.compute()
            return
        return out

    def augment_options(self, storage_options):
        """Get composite storage options

        :param dict storage_options: for a given call, these will take precedence

        :returns: options with defaults baked in
        :rtype: dict
        """
        base = self.storage_options.copy()
        if storage_options is not None:
            base.update(storage_options)
        return base

    def read_dask(self, storage_options=None, check_complete=True, **kwargs):
        if check_complete and self.flag and not self.exists():
            raise FileNotFoundError("Task not yet run or incomplete")

        return self._read(
            self.get_path_for_read(),
            storage_options=self.augment_options(storage_options),
            **kwargs
        )

    def get_path_for_read(self):
        if self.glob:
            return self._join(self.path, self.glob)
        return self.path.rstrip(self._get_sep())

    def get_path_for_write(self):
        return self.path.rstrip(self._get_sep())

    def write_dask(
        self,
        collection,
        compute=True,
        storage_options=None,
        logger=luigi_logger,
        **kwargs
    ):
        if logger:
            logger.info("Writing dask collection to {}".format(self.path))
        storage_options = self.augment_options(storage_options)
        out = self._write(
            collection,
            self.get_path_for_write(),
            storage_options=storage_options,
            compute=False,
            **kwargs
        )

        if self.flag:
            out = self.mark_complete(_dep=out, compute=False)

        if compute:
            out.compute()
            if logger:
                logger.info(
                    "Successfully wrote to {}, flagging complete".format(self.path)
                )
            return None
        return out

    # Abstract interface: implement for various storage formats

    @classmethod
    def _read(cls, path, **kwargs):
        raise NotImplementedError()

    @classmethod
    def _write(cls, collection, path, **kwargs):
        raise NotImplementedError()


class ParquetTarget(BaseDaskTarget):
    ...


class CSVTarget(BaseDaskTarget):
    ...
```

Note that these targets force you to specify directory datasets with an ending
`/`; Dask (annoyingly) is inconsistent on this, so you may find yourself
manipulating paths inside ParquetTarget and CSVTarget differently.  The user of
these targets should not need to worry about these details!

### Dask Analysis

Implement your luigi tasks in `pset_5.tasks`

#### Yelp Reviews
The data for this problem set is here:
```bash
$ aws s3 ls s3://cscie29-data/pset5/yelp_data/
2019-03-29 16:35:53    6909298 yelp_subset_0.csv
...
2019-03-29 16:35:54    7010664 yelp_subset_19.csv
```
***Do not copy the data! Everything will be done with dask via S3.***

Write an ExternalTask named `YelpReviews` which uses the appropriate dask
target.

#### Clean the data
We will turn the data into a parquet data set and cache it locally.  Start with
something like:

```python
class CleanedReviews(Task):
    subset = BoolParameter(default=True)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    def run(self):

        numcols = ["funny", "cool", "useful", "stars"]
        dsk = self.input().read_dask(...)

        if self.subset:
            dsk = dsk.get_partition(0)

        out = ...
        self.output().write_dask(out, compression='gzip')
```

Note that we turn on `subset` by default to limit bandwidth.  You will run the
full dataset from your computer only and only to provide the direct answers
to the quiz.

Notes on cleaning:

* All computation should use Dask.  Do not compute to a pandas DF (you may use
map_partitions etc)

* Ensure that the `date` column is parsed as a pandas datetime using
[parse_dates](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)

* The columns `["funny", "cool", "useful", "stars"]` are all inherently
integers. However, given there are missing values, you must first read them as
floats, fill nan's as 0, then convert to int.  You can provide a dict of
`{col: dtype}` when providing the dtype arg in places like `read_parquet` and
`astype`

* There are about 60 rows (out of 200,000) that are corrupted which you can
drop. They are misaligned, eg the text shows up in the row id.  You can find
them by dropping rows where `user_id` is null or the length of `review_id`
is not 22.

* You should set the index to `review_id` and ensure the output reads back with
meaningful divisions

#### Analysis
Let's look at how the length of a review depends on some factors.  By length,
let's just measure the number of characters (no need for text parsing).

##### CLI
Running `python -m pset_5` on your master branch in Travis should display the
results of each of these ***on the subset of data***.

You should add a flag, eg `python -m pset_5 --full` to run and display the
results on the entire set.  Use these for your quiz responses locally.

The tasks should run and read back their results for printing, eg:

```python
class BySomething(Task):

    # Be sure to read from CleanedReviews locally

    def run(self):
        ...

    def print_results(self):
        print(self.output().read_dask().compute())
```

You can restructure using parent classes if you wish to capture some of the
patterns.

A few tips:

* You may need to use the `write_index` param in `to_parquet` after a `groupby`
given the default dask behavior. See
[to_parquet](http://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.to_parquet)

* You cannot write a `Series` to parquet. If you have something like
`df['col'].groupby(...).mean()` then you need to promote it back to a dataframe,
like `series.to_frame()`, or `df[['col']]` to keep it as a 1-column frame to
begin with.

* Round all your answers and store them as integers.  Report the integers in
Canvas.

* Consider using pandas/dask [text
tools](https://pandas.pydata.org/pandas-docs/stable/user_guide/text.html) like
`df.str`

* Only load the columns you need from `CleanedReviews`!  No need to load all
columns every time.

##### ByDecade

What is the average length of a review by the decade (eg `2000 <= year < 2010`)?

##### ByStars
What is the average length of a review by the number of stars?
>>>>>>> origin/master
