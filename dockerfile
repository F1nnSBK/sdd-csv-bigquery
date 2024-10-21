FROM docker.io/library/python:3.10.9-alpine

ENV PYTHONUNBUFFERED True

COPY requirements.txt .

RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir --requirement requirements.txt

COPY . .

EXPOSE 8080

CMD ["python3", "app.py"]