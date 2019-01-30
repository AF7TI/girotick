FROM python:latest

RUN pip install --upgrade pip

WORKDIR /usr/local/tick

COPY * ./

RUN pip install -r requirements.txt

CMD [ "python", "./tick.py" ]
