FROM python:3.8

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 5000

CMD [ "gunicorn", "-k", "gthread", "--threads", "8", "-b", "0.0.0.0:5000", "--access-logfile", "-", "--keep-alive", "60", "main" ]