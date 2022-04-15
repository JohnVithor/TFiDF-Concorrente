''' id_inserter.py '''

import csv
import pandas as pd
import vaex

NAME='train_id'

def main():
    '''asd'''
    data = pd.read_csv(f"../datasets/{NAME}.csv", names=['id','title', 'text'])
    data.to_csv(f"../datasets/{NAME}.csv", index=False, quoting=csv.QUOTE_ALL, header=False)


def main1():
    ''' main function'''
    data = vaex.from_csv(f"../datasets/{NAME}.csv", names=['type', 'title', 'text'])
    data.drop('type', axis=1, inplace=True)
    data.export_csv(f"../datasets/{NAME}_id.csv", progress=True, header=False)

if __name__ == "__main__":
    main()
