FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY page.py .


EXPOSE 443

ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=443

CMD ["streamlit", "run", "page.py", "--server.address=0.0.0.0", "--server.port=443"]