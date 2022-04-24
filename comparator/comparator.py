"""comparator.py"""

import sys
import vaex
import numpy as np

FILENAME='devel_1_000_id'


def calcular_speedup(one_time:int, two_time:int):
    """
    Calcula qual foi o speedup, fornecidos os tempos de dois programas
    """
    return one_time / two_time


def main(path_one, path_two, name):
    """
    Função principal do programa
    """

    one = vaex.open(f"{path_one}/{name}_tfidf_results.parquet")

    one.sort(by=['doc', 'term'])

    two = vaex.open(f"{path_two}/{name}_tfidf_results.parquet")
    two.sort(by=['doc', 'term'])

    comparisson_data = one.compare(two)
    result_comparisson = np.all([not l for l in comparisson_data])
    print("Os resultados dos dois arquivos são esquivalentes?", result_comparisson)

    if not result_comparisson:
        print(comparisson_data)
        del one
        del two
        del comparisson_data
        sys.exit()

    del one
    del two
    del comparisson_data

    with open(f"{path_one}/output_{name}.log", encoding="UTF-8") as log_one,\
    open(f"{path_two}/output_{name}.log", encoding="UTF-8") as log_two:
        # Primeira parte
        print("Considerando a primeira parte do algoritmo")
        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(one_time, two_time))

        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os segundos foi de:", calcular_speedup(one_time, two_time))

        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os minutos foi de:", calcular_speedup(one_time, two_time))

        # Segunda Parte
        print("\n\n")
        print("Considerando a segunda parte do algoritmo")
        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(one_time, two_time))

        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os segundos foi de:", calcular_speedup(one_time, two_time))

        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os minutos foi de:", calcular_speedup(one_time, two_time))

        # Total
        print("\n\n")
        print("Considerando o total do algoritmo")
        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(one_time, two_time))

        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os segundos foi de:", calcular_speedup(one_time, two_time))

        one_time = int(log_one.readline())
        two_time = int(log_two.readline())
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(one_time-two_time))
        if two_time != 0:
            print("O speedup considerando os minutos foi de:", calcular_speedup(one_time, two_time))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise Exception("Quantidade de argumentos inválida")
    main(sys.argv[1], sys.argv[2], sys.argv[3])
