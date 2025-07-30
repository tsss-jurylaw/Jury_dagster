FROM python:3.10

RUN mkdir /app

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt --force-reinstall


RUN apt-get update && apt-get install -y


ADD . /app/

EXPOSE 4006

CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "4006"]
