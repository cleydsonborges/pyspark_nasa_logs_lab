FROM docker.geofusion.com.br/spark-pandas

RUN conda install -y -c conda-forge pyarrow && \
    apk add --no-cache --update libexecinfo libexecinfo-dev snappy snappy-dev

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r requirements.txt && \
    rm -rf ~/.cache/pip

COPY scripts/ ./pyspark-nasa-logs-lab/scripts/
COPY resources/ ./pyspark-nasa-logs-lab/resources/

WORKDIR '/pyspark-nasa-logs-lab/scripts/'

CMD [ "python", "./app.py" ]