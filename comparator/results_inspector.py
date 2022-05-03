''' results_inspector.py '''

import sys
import vaex

def jv(file_path):
    ''' jv function'''
    data = vaex.open(file_path)
    data.sort(by=['doc', 'term'])
    data.export_csv(file_path+".csv", progress=True, header=False)

if __name__ == "__main__":
    jv(sys.argv[1])
