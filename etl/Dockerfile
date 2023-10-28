# Dockerfile
FROM python:3.9

COPY . .

RUN pip install -r requirements.txt
RUN apt-get update

#Installation supplémentaire de 'locale' en français, pour bien récupérer les mois en français
RUN apt-get install -y locales locales-all
ENV LC_ALL fr_FR.UTF-8
ENV LANG fr_FR.UTF-8
ENV LANGUAGE fr_FR.UTF-8
RUN dpkg-reconfigure locales

WORKDIR /
CMD ["python", "main.py"]
