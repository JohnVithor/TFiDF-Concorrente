"""comparator.py"""

import sys
import vaex
import numpy as np

def main(path_one, path_two, name):
    """
    Função principal do programa
    """

    one = vaex.open(f"{path_one}/{name}_tfidf_results.parquet")
    two = vaex.open(f"{path_two}/{name}_tfidf_results.parquet")

    comparisson_data = one.sort(by=['doc', 'term']).compare(two.sort(by=['doc', 'term']))
    result_comparisson = np.all([not l for l in comparisson_data])
    print(comparisson_data)
    print("Os resultados dos dois arquivos são esquivalentes?", result_comparisson)

    del one
    del two
    del comparisson_data

if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise Exception("Quantidade de argumentos inválida")
    main(sys.argv[1], sys.argv[2], sys.argv[3])
