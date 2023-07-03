FROM python:3.11.3-bullseye

RUN apt-get update && \
    apt-get install -y curl

RUN pip install --upgrade pip

# Copy scripts
COPY main.py /crypto-transform/
COPY requirements.txt /crypto-transform/

WORKDIR /crypto-transform

RUN pip install -r requirements.txt

# Download logger binary file
RUN curl https://storage.googleapis.com/datatask-public-packages/log-streamer --output /crypto-transform/log-streamer

RUN chmod +x /crypto-transform/log-streamer

CMD ["/crypto-transform/log-streamer", "python", "/crypto-transform/main.py"]
