FROM python:3.9
RUN mkdir /racing-reference
WORKDIR /racing-reference
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade -r /requirements.txt
COPY . /racing-reference/
CMD [ "python", "main.py"]


