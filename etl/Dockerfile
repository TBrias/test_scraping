# Dockerfile pour le conteneur unique
FROM python:3.9
# Copiez tous les fichiers et répertoires nécessaires
COPY . .

RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get install -y locales locales-all
ENV LC_ALL fr_FR.UTF-8
ENV LANG fr_FR.UTF-8
ENV LANGUAGE fr_FR.UTF-8
RUN dpkg-reconfigure locales
# Exécutez le script principal

WORKDIR /etl/predictice_test/predictice_scrapy
CMD ["scrapy", "crawl legifrance_spider"]