FROM python:3.11-slim

WORKDIR /app

COPY data_app/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY data_app/page.py .
COPY shared ./shared

EXPOSE 80

ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=80

CMD ["streamlit", "run", "page.py", "--server.address=0.0.0.0", "--server.port=80"]
