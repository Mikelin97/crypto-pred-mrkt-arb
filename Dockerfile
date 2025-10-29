FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY data_stream_test.py .

CMD ["python", "data_stream_test.py"]