"""comparator.py"""

import sys
import pandas as pd

FILENAME='test_id'


def calcular_speedup(serial_time:int, concurrent_time:int):
    """
    Calcula qual foi o speedup, fornecidos os tempos de dois programas,
    o primeiro serial e o segundo concorrente
    """
    return serial_time / concurrent_time


def main():
    """
    Função principal do programa
    """
    # Testando equivalencia dos resultados das versoes seriais e concorrentes

    serial = pd.read_parquet(f"../results_serial/{FILENAME}_tfidf_results.parquet")
    serial.sort_values(['doc', 'term'], inplace=True)
    serial.reset_index(drop=True, inplace=True)

    concurrent = pd.read_parquet(f"../results_concurrent/{FILENAME}_tfidf_results.parquet")
    concurrent.sort_values(['doc', 'term'], inplace=True)
    concurrent.reset_index(drop=True, inplace=True)

    result_comparisson = serial.equals(concurrent)
    print("Os resultados da versão serial e da versão concorrente " \
    f"são equivalentes no arquivo {FILENAME}?", result_comparisson)

    if not result_comparisson:
        df_all = serial.merge(concurrent, on=['doc','term'],
                   how='outer', indicator=True)
        print(df_all[df_all['value_x'] != df_all['value_y']])
        del serial
        del concurrent
        sys.exit()



    del serial
    del concurrent

    with open(f"../logs_serial/output_{FILENAME}.log", encoding="UTF-8") as log_serial,\
    open(f"../logs_concurrent/output_{FILENAME}.log", encoding="UTF-8") as log_concurrent:
        print(f"O tamanho do vocabulario nas duas versões, considerando o arquivo {FILENAME}"\
            ", é o mesmo?", log_serial.readline() == log_concurrent.readline())
        print(f"O número de documentos nas duas versões, considerando o arquivo {FILENAME}"\
            ", é o mesmo?", log_serial.readline() == log_concurrent.readline())
        # print("A diferença de tempo médio em nanosegundos de processamento de cada documento" \
        # " foi de:", abs(int(log_serial.readline().split(':')[1])-int(log_concurrent.readline().split(':')[1])))
        # print("A diferença de tempo médio em milisegundos de processamento de cada documento" \
        # " foi de:", abs(int(log_serial.readline().split(':')[1])-int(log_concurrent.readline().split(':')[1])))
        # print("A diferença de tempo médio em segundos de processamento de cada documento" \
        # " foi de:", abs(int(log_serial.readline().split(':')[1])-int(log_concurrent.readline().split(':')[1])))
        # print("A diferença de tempo médio em minutos de processamento de cada documento" \
        # " foi de:", abs(int(log_serial.readline().split(':')[1])-int(log_concurrent.readline().split(':')[1])))

        serial_time = int(log_serial.readline().split(':')[1])
        concurrent_time = int(log_concurrent.readline().split(':')[1])
        print("A diferença de tempo médio em nanosegundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print(serial_time, concurrent_time)
            print("O speedup considerando os nanosegundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline().split(':')[1])
        concurrent_time = int(log_concurrent.readline().split(':')[1])
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print(serial_time, concurrent_time)
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline().split(':')[1])
        concurrent_time = int(log_concurrent.readline().split(':')[1])
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print(serial_time, concurrent_time)
            print("O speedup considerando os segundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline().split(':')[1])
        concurrent_time = int(log_concurrent.readline().split(':')[1])
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print(serial_time, concurrent_time)
            print("O speedup considerando os minutos foi de:", calcular_speedup(serial_time, concurrent_time))

if __name__ == "__main__":
    main()
