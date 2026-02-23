FROM python:3.12-slim
WORKDIR /feature-engine
COPY requirements.txt .
COPY engine.py .
RUN apt-get update && apt-get install -y gcc librdkafka-dev python3-dev
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENV PYTHONUNBUFFERED=1
EXPOSE 5000
CMD ["python", "engine.py"]