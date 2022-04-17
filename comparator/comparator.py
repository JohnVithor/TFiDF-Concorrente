"""comparator.py"""

import sys
import vaex
import numpy as np

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

    serial = vaex.open(f"../results_serial/{FILENAME}_tfidf_results.parquet")
    
    serial.sort(by=['doc', 'term'])
    # serial.reset_index(drop=True)

    concurrent = vaex.open(f"../results_concurrent/{FILENAME}_tfidf_results.parquet")
    concurrent.sort(by=['doc', 'term'])
    # concurrent.reset_index(drop=True)

    comparisson_data = serial.compare(concurrent)
    result_comparisson = np.all([not l for l in comparisson_data])
    print("Os resultados da versão serial e da versão concorrente " \
    f"são equivalentes no arquivo {FILENAME}?", result_comparisson)

    if not result_comparisson:
        print(comparisson_data)
        del serial
        del concurrent
        del comparisson_data
        sys.exit()

    del serial
    del concurrent
    del comparisson_data

    with open(f"../logs_serial/output_{FILENAME}.log", encoding="UTF-8") as log_serial,\
    open(f"../logs_concurrent/output_{FILENAME}.log", encoding="UTF-8") as log_concurrent:
        
        # Primeira parte
        print("Considerando a primeira parte do algoritmo")
        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os segundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os minutos foi de:", calcular_speedup(serial_time, concurrent_time))

        # Segunda Parte
        print("\n\n")
        print("Considerando a segunda parte do algoritmo")
        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os segundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os minutos foi de:", calcular_speedup(serial_time, concurrent_time))

        # Total
        print("\n\n")
        print("Considerando o total do algoritmo")
        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em milisegundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os milisegundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em segundos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os segundos foi de:", calcular_speedup(serial_time, concurrent_time))

        serial_time = int(log_serial.readline())
        concurrent_time = int(log_concurrent.readline())
        print("A diferença de tempo médio em minutos de processamento total dos documentos" \
        " foi de:", abs(serial_time-concurrent_time))
        if concurrent_time != 0:
            print("O speedup considerando os minutos foi de:", calcular_speedup(serial_time, concurrent_time))

if __name__ == "__main__":
    main()
