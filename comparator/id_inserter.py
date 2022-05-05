''' id_inserter.py '''

import sys
import csv
import vaex

def main(input_path, output_path):
    ''' main function'''
    data = vaex.from_csv(input_path, names=['id', 'title', 'text'])
    data['id'] = vaex.vrange(0, len(data), dtype=int)
    data.export_csv(output_path, progress=True, sep=';', quoting=csv.QUOTE_ALL, header=False)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise Exception("Quantidade de argumentos inválida, escreva apenas o input_path e o output_path")
    main(sys.argv[1], sys.argv[2])
