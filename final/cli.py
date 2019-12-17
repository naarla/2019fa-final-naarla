from luigi import build
from final.luigi import DownloadImage, ContentImage
#CleanedReviews
from final.stylize import  ProcessImages, BySegment, OutputStorage

import argparse

parser = argparse.ArgumentParser(description='Yelp Reviews')
parser.add_argument("-f", "--full", action="store_false", dest="full")


def main(args=None):
    args = parser.parse_args(args=args)
    build([
           # DownloadImage(image="luigi.jpg"),
           # Stylize()
            ProcessImages(),

           # ByImageType(),
            OutputStorage(),
            BySegment()
           # BySegment()

           ]


          , local_scheduler=True)


