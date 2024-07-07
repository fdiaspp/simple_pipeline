FROM apache/spark-py:v3.4.0

WORKDIR /app

CMD [ "/opt/spark/bin/spark-submit", "/app/main.py" ]