# TFiDF-Concorrente

Disciplina: DIM0124 - Programação Concorrente
Professor: Nélio Alessandro Azevedo Cacho

## Origem do Dataset 
 - https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews

## Organização do projeto

O projeto está organizado do seguinte modo:
 - datasets: possui arquivos com: 
   - 1000, 10.000, 100.000 documentos (testes rapidos)
   - test(177 MB), train(1,6 MB) (testes mais completos)
 - diagrams: local onde os diagramas de classes de cada abordagem são armazenados
 - results: local onde resultados de alguns testes são armazenados
 - src: local do código

O Código em específico está organizado com os seguintes pacotes:
 - microbenchmark: implementação dos testes, adaptando o algoritmo para ser compativel com o JMH, utilizando o State para viabilizar os testes (não considerando o armazenamento dos resultados)
 - records: classes de dados para auxiliar algumas operações
 - tfidf: implementação das diferentes abordagens para o TFiDF (considerando o armazenamento dos resultsdos)
 - utils: Algumas classes auxiliares, implementam funções envolvendo a manipulação de strings:
   - considerando o uso da StringUtils da Apache e apenas o Regex do próprio Java
   - Outras classes não envolvidas na manipulação de strings também se encontram aqui:
     - Buffer: Para comunicação entre threadsvia producer-consumer
     - Writer: Para armazenamento dos resultados em arquivo .parquet

## Como executar:
 - Para executar os micro benchmarks, utilize o main da classe Microbenchmark presente no package microbenchmark
 - Para alterar o plano de execução (State) acesse a classe ExecutionPlan presente no package microbenchmark
 - Para executar os testes considerando a escrita de arquivo (Sem micro benchmark):
   - Utilize o main da classe relativa a abordagem escolhida, alterando os parametros para os desejados
   - Exemplo: Naive Serial ou Naive Concurrent (Threads com Producer-Consumer)

Autor: João Vitor Venceslau Coelho