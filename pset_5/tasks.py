import pandas as pd
from luigi import ExternalTask, Task, LocalTarget
from luigi import BoolParameter
import os
from csci_utils.luigi import target
from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import Requires, Requirement, TargetOutput





class YelpReviews(ExternalTask):
    '''Write an ExternalTask named YelpReviews which uses the appropriate dask target'''
    def output(self):
        print("Yelp Reviews is called output here")
        file_path = "s3://ohsunny/pset_5/yelp/"
        output = CSVTarget(file_path, flag=None, glob= "*.csv")

        return  output


class CleanedReviews(Task):
    subset = BoolParameter(default=True)
    requires = Requires()
    yelp_reviews = Requirement(YelpReviews)
    print("Cleaned Reviews has been called")

    output = TargetOutput( ext="/", target_class=ParquetTarget, glob="*.parquet")

    def run(self):
        print ("run has been called")

        numcols = ["funny", "cool", "useful", "stars"]
        dsk = self.input()["yelp_reviews"].read_dask(storage_options=dict(requester_pays=True),
                                                     dtype={col:"float64" for col in numcols},
                                                     parse_dates=['date'])
        # dsk = self.input()["reviews"].read_dask(storage_options=dict(requester_pays=True), dtype={col:"float64" for col in numcols}, parse_dates=['dates'])

        print (dsk.head())

        if self.subset:
            dsk = dsk.get_partition(0)


        # dsk = dsk.merge(df2.set_index('CPT Code'), on='CPT Code')
        out = (dsk.dropna(subset=["user_id", "date"])[dsk["review_id"].str.len()==22]
            .set_index("review_id")
            .fillna(value={col: 0.0 for col in numcols})
            .astype({col:"int32" for col in numcols})
        )
        out["length_reviews"] = out["text"].str.len()
        self.output().write_dask(out, compression='gzip')
        print(out.head())



class ByDecade(Task):
    subset = BoolParameter(default=True)
   # requires=Requires()
    decade_reviews=Requirement(CleanedReviews)
    print("before output function")
    output = TargetOutput( ext="/by-decade/", target_class=ParquetTarget, glob="*.parquet")
    print("output is", output)

    def requires(self):
        return CleanedReviews(subset=self.subset)

    def run(self):
        print("got to run")
        dsk = self.input().read_dask(
            columns=["date", "length_reviews"],
            parse_dates=['date'])\
            .set_index("date")

        if self.subset:
            dsk = dsk.get_partition(0)

        out = (
                dsk.groupby((dsk.index.year//10) *10)["length_reviews"]
            .mean()
            .to_frame()
            .astype({"length_reviews":"int32"})
               )


        self.output().write_dask(out, compression='gzip')
        self.print_results()
        print("the above should be results by decade", out)

    def print_results(self):
        print("visited print results")
        print(self.output().read_dask().compute())

class ByStars(Task):
    subset = BoolParameter(default=True)
    requires=Requires()
    cleaned_review=Requirement(CleanedReviews)
    output = TargetOutput( ext="/by-stars/", target_class=ParquetTarget, glob="*.parquet")
    #output = TargetOutput(file_pattern=os.path.abspath("./data/by-stars"), target_class=ParquetTarget, ext="")
    #output = TargetOutput( ext='files.parquet/', target_class=ParquetTarget, glob="*.parquet")

    def run(self):
        dsk = self.input()["cleaned_review"].read_dask(columns=["length_reviews", "stars"])

        if self.subset:
            dsk = dsk.get_partition(0)

        out = (
            dsk.groupby("stars")["length_reviews"]
            .mean()
            .to_frame()
            .astype({"length_reviews":"int32"})
        )

        self.output().write_dask(out, compression='gzip')
        self.print_results()

    def print_results(self):
        print(self.output().read_dask().compute())





