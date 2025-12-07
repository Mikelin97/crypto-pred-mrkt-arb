FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app/db/data_initialization

# Install only what this loader needs
COPY db/data_initialization/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Keep the expected repo-style paths so useful_functions can be imported
COPY db/data_initialization/main.py /app/db/data_initialization/main.py
COPY useful_functions.py /app/useful_functions.py

ENTRYPOINT ["python", "/app/db/data_initialization/main.py"]
