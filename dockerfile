FROM docker.io/library/python:3.10.9-alpine

ENV PYTHONUNBUFFERED True

WORKDIR /app

COPY requirements.txt .

RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir --requirement /app/requirements.txt

COPY . .

EXPOSE 8080

ENTRYPOINT ["gunicorn", "--bind", ":8080", "--workers", "2", "--threads", "8", "--timeout", "0", "--log-level=DEBUG", "app:app"]