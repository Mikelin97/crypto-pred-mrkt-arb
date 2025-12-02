FROM python:3.11-slim

WORKDIR /app

COPY analytics/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY shared ./shared
COPY analytics/order_book_vis.py .


EXPOSE 80

ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=80

CMD ["streamlit", "run", "order_book_vis.py", "--server.address=0.0.0.0", "--server.port=80"]
