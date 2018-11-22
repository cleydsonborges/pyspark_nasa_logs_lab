# Utilizando PySpark para Análise dos logs (NASA)

O objetivo dessa aplicação é responder algumas perguntas a partir da análise dos logs das requisições  feitas ao site da NASA.
Por padrão o arquivo de dados está no formato Apache Common Log Format (CLF).
ex:

dialup-2-139.gw.umn.edu - - [01/Aug/1995:00:36:30 -0400] "GET /facilities/mlp.html HTTP/1.0" 200 2653


### Pré Requisitos

1 -  Para execução do projeto é  necessário ter o Docker instalado em sua máquina loca.
Para maiores instruções de sua instalação acesse o link abaixo:
https://docs.docker.com/install/linux/docker-ce/ubuntu/#set-up-the-repository

2 - Efetuar Download dos arquivos de Logs do  site da NASA, descompactá-los na pasta resources.
ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz ,
ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz​
Temos uma amostra de dados já  no repositório. TODO: Mapeamento de Volume para Bucket S3.

### Executando Jupyter Notebook.

```
make deploy_jupyter_notebook
```
ou
```
docker run -it --rm -v $(ROOT_DIR):/home/jovyan/work --user root -p 8888:8888 jupyter/all-spark-notebook
```

### Executando Aplicação em Python

1 -  Construir a imagem base
```
make build
```
ou
```
docker build -t pyspark-nasa-logs-lab .
```

2 - Execução dos testes
```
make tests
```

3 - Rodar aplicação
```
make deploy_container
```
ou
```
docker run -it --rm -v $(ROOT_DIR)resources:/pyspark-nasa-logs-lab/resources -v $(ROOT_DIR)scripts:/pyspark-nasa-logs-lab/scripts pyspark-nasa-logs-lab
```

Ao rodar a aplicação Python será  gerado um arquivo de saída no diretório scripts/output.txt com as análises prontas.





