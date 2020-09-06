FROM python:3.7
RUN  python --version

RUN apt update

RUN apt install make -y
RUN apt install zip -y
RUN apt install unzip -y

# Create app directory
WORKDIR /app

# Bundle app source
COPY ./ ./

# Install app dependencies
RUN pip install -r requirements.txt

# Install jdk
RUN apt install default-jdk -y