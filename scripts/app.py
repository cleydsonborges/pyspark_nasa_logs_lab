import re
import datetime
import os
from pyspark import SparkContext
from pyspark.sql import Row

sc = SparkContext(appName="Nasa")

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Converte a string de data do log em um objeto datetime do python
    Args:
        s (str): date e time como string
    Returns:
        datetime: datetime object (ignora o timezone)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))

def parse_apache_log_line(logline):
    """ Faz o parse da linha no formato Apache Common Log
    Args:
        logline (str): uma linha em texto no formato Apache Common Log
    Returns:
        tuple: retorna um dicionário com atributos dos logs e status 1 para linhas
        que foram parseadas com sucesso e 0 para falhas
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = int(0)
    else:
        size = int(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

# Padrão da expressão para extrair os dados de logs de maneira estruturada.
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

def parseLogs():
    """ Ler o Arquivo e fazer o parse do LOG """
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parse_apache_log_line)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print('Número de linhas inválidas no log: {}'.format(failed_logs.count()))
        for line in failed_logs.take(20):
            print('Invalid logline: {}'.format(line))


    print('{} linhas lidas, {} linhas parseadas com sucesso, {} linhas falharam o parse'.format(parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs

if __name__ == "__main__":
#TODO: Parametrizar via args o diretório dos arquivos.
    baseDir = os.path.join('../')
    inputPath = os.path.join('resources')
    logFile = os.path.join(baseDir, inputPath)

    parsed_logs, access_logs, failed_logs = parseLogs()

    hosts = access_logs.map(lambda log: log.host)
    unique_hosts = hosts.distinct()
    unique_host_count = unique_hosts.count()
    print('1. Número de hosts únicos.: {}'.format(unique_host_count))

    bad_records = access_logs.filter(lambda log: log.response_code == 404).cache()
    print('2. O total de erros 404.: {}'.format(bad_records.count()))

    err_404 = access_logs.filter(lambda log: log.response_code == 404)
    endpoint_count_pair_tuple = err_404.map(lambda log: (log.endpoint, 1))
    endpoint_sum = endpoint_count_pair_tuple.reduceByKey(lambda a, b: a + b)
    five_top_err_urls = endpoint_sum.takeOrdered(5, lambda s: -1 * s[1])
    print('3. As 5 URLs que mais causaram erro 404.: {}'.format(five_top_err_urls))

    err_date_count_pair_tuple = bad_records.map(lambda log: (log.date_time.day, 1))
    err_date_sum = err_date_count_pair_tuple.reduceByKey(lambda a, b: a + b)
    err_date_sorted = err_date_sum.sortByKey().cache()
    err_by_date = err_date_sorted.collect()
    print('4. Quantidade de erros 404 por dia.: {}'.format(err_by_date[0][1]))

    content_sizes = access_logs.map(lambda log: log.content_size).cache()
    content_sizes = content_sizes.reduce(lambda a, b: a + b)
    print('5. O total de bytes retornados.: {}'.format(content_sizes))

    with open('output.txt', 'a+') as file:
        file.write('1. Número de hosts únicos.: {}\n'.format(unique_host_count))
        file.write('2. O total de erros 404.: {}\n'.format(bad_records.count()))
        file.write('3. As 5 URLs que mais causaram erro 404.: {}\n'.format(five_top_err_urls))
        file.write('4. Quantidade de erros 404 por dia.: {}\n'.format(err_by_date[0][1]))
        file.write('5. O total de bytes retornados.: {}\n'.format(content_sizes))