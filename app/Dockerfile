FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 8501

COPY page.py .

CMD ["streamlit", "run", "page.py"]