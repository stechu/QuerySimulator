#!/usr/bin/env python

import argparse
import csv

#parse args
parser = argparse.ArgumentParser(
    description='collect running time of workers of a query')
parser.add_argument("-o", "--output", type=str, help="input file")
parser.add_argument("-i", "--input", type=str, help="output file")
parser.add_argument("-s", "--shuffle",
                    help="shuffle file or not", action="store_true")
args = parser.parse_args()


# sort events according to its time stamp
def sort_events(sortfunc):
    with open(args.input, 'rb') as f:
        csvreader = csv.reader(f)
        events = [row for row in csvreader]
        sorted(events, key=sortfunc)
    with open(args.output, 'wb') as w:
        csvwriter = csv.writer(w)
        for event in events:
            print event
            csvwriter.writerow(event)


def main():
    print args.output
    print args.input
    if args.shuffle:
        sort_events(lambda row: long(row[3]))
    else:
        sort_events(lambda row: long(row[2]))


if __name__ == "__main__":
    main()
