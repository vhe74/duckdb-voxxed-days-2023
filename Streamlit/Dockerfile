FROM python:3.11.3-bullseye

RUN apt-get update && \
    apt-get install -y curl

RUN pip install --upgrade pip

RUN mkdir /streamlit

COPY ./streamlitapp.py /streamlit/
COPY ./requirements.txt /streamlit/

WORKDIR /streamlit

RUN pip install -r requirements.txt

# Download grpx binary file
RUN curl https://storage.googleapis.com/datatask-public-packages/grpx:latest --output /streamlit/grpx

RUN chmod u+x /streamlit/grpx

# Download logger binary file
RUN curl https://storage.googleapis.com/datatask-public-packages/log-streamer --output /streamlit/log-streamer

RUN chmod +x /streamlit/log-streamer

CMD ["/streamlit/log-streamer", "/streamlit/grpx"]