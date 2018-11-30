FROM python:latest

WORKDIR /usr/local/tick

COPY * ./

RUN pip install -r requirements.txt

CMD [ "python", "./tick.py" ]
