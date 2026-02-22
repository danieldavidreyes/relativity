FROM python:3.12-slim
WORKDIR /data
COPY requirements.txt .
COPY gen.py .
RUN apt-get update && apt-get install -y gcc librdkafka-dev python3-dev
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
EXPOSE 5000
CMD ["python", "gen.py"]