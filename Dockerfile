FROM python:3-slim

MAINTAINER Pål Andreassen "pal.a@sesam.io"

COPY ./service /service
WORKDIR /service

RUN pip install -r requirements.txt

EXPOSE 5000/tcp

ENTRYPOINT ["python3"]
CMD ["service.py"]