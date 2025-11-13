FROM python:3.11-slim

WORKDIR /app

COPY app/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY backtest.py useful_functions.py ./
COPY shared ./shared
COPY app/page.py .

RUN mkdir -p data/prices data/contracts


EXPOSE 80

ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=80

CMD ["streamlit", "run", "page.py", "--server.address=0.0.0.0", "--server.port=80"]
