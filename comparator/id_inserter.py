''' id_inserter.py '''

import csv
import vaex

NAME='devel_100_000_id'

def main():
    ''' main function'''
    data = vaex.from_csv(f"../datasets/{NAME}.csv", names=['id', 'title', 'text'])
    data.export_csv(f"../datasets/{NAME}.csv", progress=True, sep=';', quoting=csv.QUOTE_ALL, header=False)

if __name__ == "__main__":
    main()
