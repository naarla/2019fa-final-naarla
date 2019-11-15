from luigi import build
from pset_5.tasks import YelpReviews, CleanedReviews
from pset_5.tasks import ByStars, ByDecade

import argparse

parser = argparse.ArgumentParser(description='Yelp Reviews')
parser.add_argument("-f", "--full", action="store_false", dest="full")


def main(args=None):
    args = parser.parse_args(args=args)
    build([ByDecade(subset=args.full),
           ByStars(subset=args.full) ], local_scheduler=True)
   # build([ByDecade(subset=args.full)], local_scheduler=True)

#[ByDecade(subset=args.full)],

