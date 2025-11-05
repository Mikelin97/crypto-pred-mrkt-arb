FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY page.py .

COPY Market.py .

EXPOSE 8501

CMD ["streamlit", "run", "page.py"]