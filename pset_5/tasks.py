import pandas as pd
from luigi import ExternalTask, Task, LocalTarget
from luigi import BoolParameter
import os
from csci_utils.luigi import target
from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requires, Requirement, TargetOutput


class YelpReviews(ExternalTask):
    """Write an ExternalTask named YelpReviews which uses
     the appropriate dask target
     :returns CSVTarget - accesses the s3 aws bucket"""

    def output(self):

        return CSVTarget("s3://ohsunny/pset_5/yelp/", flag=None, glob="*.csv")


class CleanedReviews(Task):

    """Cleaned Reviews is task that takes the Yelp Reviews (from s3)
     and in the run function processes "cleans" the dask dataframe
      and turns the data into a parquet data set and cache it locally
     drops na, parses dates, changes datatype

     :returns dask dataframe to be passed on to byStars and byDecade
     outputs the parquet files into the directory clean_reviews
     """

    subset = BoolParameter(default=True)
    requires = Requires()
    reviews = Requirement(YelpReviews)

    output = TargetOutput(ext="/clean_reviews/", target_class=ParquetTarget, glob="*.parquet")

    def run(self):

        numcols = ["funny", "cool", "useful", "stars"]
        dsk = self.input()["reviews"].read_dask(
            storage_options=dict(requester_pays=True),
            dtype={col: "float64" for col in numcols},
            parse_dates=["date"],
        )
        if self.subset:
            dsk = dsk.get_partition(0)

        dsk["review_id"] = dsk["review_id"].mask(dsk["review_id"].str.len() != 22)
        out = (
            dsk.dropna(subset=["user_id", "date"])
            .set_index("review_id")
            .fillna(value={col: 0.0 for col in numcols})
            .astype({col: "int32" for col in numcols})
        )

        out["length_reviews"] = out["text"].str.len()
        self.output().write_dask(out, compression="gzip")


class ByDecade(Task):
    """Computes is the average length of a review by the decade
    :returns output in the directory a parquet file that contains
    number of reviews by decade in the by-decade directory"""

    subset = BoolParameter(default=True)
    decade_reviews = Requirement(CleanedReviews)

    output = TargetOutput(
        ext="/by-decade/", target_class=ParquetTarget, glob="*.parquet"
    )

    def requires(self):
        return CleanedReviews(subset=self.subset)

    def run(self):

        dsk = (
            self.input()
            .read_dask(columns=["date", "length_reviews"], parse_dates=["date"])
            .set_index("date")
        )

        if self.subset:
            dsk = dsk.get_partition(0)

        out = (
            dsk.groupby((dsk.index.year // 10) * 10)["length_reviews"]
            .mean()
            .to_frame()
            .astype({"length_reviews": "int32"})
        )

        ByStars.print_results(self, out)


class ByStars(Task):
    """Computes the average length of a review by the number of stars
    :returns output in the directory a parquet file that contains
    number of reviews by stars by-stars directory"""

    subset = BoolParameter(default=True)
    requires = Requires()
    cleaned_review = Requirement(CleanedReviews)
    output = TargetOutput(
        ext="/by-stars/", target_class=ParquetTarget, glob="*.parquet"
    )

    def run(self):
        dsk = self.input()["cleaned_review"].read_dask(
            columns=["length_reviews", "stars"]
        )

        if self.subset:
            dsk = dsk.get_partition(0)

        out = (
            dsk.groupby("stars")["length_reviews"]
            .mean()
            .to_frame()
            .astype({"length_reviews": "int32"})
        )

        self.print_results(out)

    def print_results(self, out):
        self.output().write_dask(out, compression="gzip")
        print(self.output().read_dask().compute())
