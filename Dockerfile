FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY StreamCryptoPrice.py .

CMD ["python", "StreamCryptoPrice.py"]