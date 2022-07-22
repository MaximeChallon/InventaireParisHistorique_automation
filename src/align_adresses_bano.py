import dotenv
import re
import json
import requests
import sqlite3
import pandas as pd
import csv
import numpy as np
import urllib.parse
from utils.adresses import *


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))

DEBUG = os.environ['DEBUG']
KEY_WS = os.environ["KEY_WS"]
URL_ROOT  = os.environ["URL_ROOT"]
CSV_BANO75  = os.environ["CSV_BANO75"]
CSV_INVENTAIRE = os.environ["CSV_INVENTAIRE"]
BASE_ADRESSES = BASE_DIR + "/out/base_adresses.sqlite"
constantes_webapp = "/home/maxime/dev/InventaireParisHistorique_webapp/app/constantes.py"
RUES = None
with open(constantes_webapp, "r") as f:
    for line in f:
        if line.startswith("RUE =") or line.startswith("RUE="):
            RUES= (re.sub(r"^RUE ?= ?", "", line)).replace("[", "").replace("]", "").replace('"', "").replace("\n", "").split("),(")

RUES = list(filter(None, [(re.sub(r", ?$","",rue[:(int(len(rue.replace("(", "").replace(")", ""))/2))]) if rue != "" and rue !="(" and rue != "(," and rue != "(, " else None) for rue in RUES]))

if not (os.path.exists(BASE_ADRESSES)):
    open(BASE_ADRESSES, "w").close()    
conn = sqlite3.connect(BASE_ADRESSES)  
curs = conn.cursor()

#obtenir les adresses distinctes avec leur éventuelle géoloc et les mettre dans nouvelle base
df = pd.read_csv(CSV_INVENTAIRE, delimiter="|",  usecols=['N_rue', 'Rue', 'Ville', 'Arrondissement', 'Latitude_x', 'Longitude_y'])
df  = df.drop_duplicates(['N_rue', 'Rue', 'Ville', 'Arrondissement'])
df = df.replace({np.nan:None})

# ETAPE 1

# créer les tables 
curs.execute("""
CREATE TABLE IF NOT EXISTS `adresse` (
  `adresseid` VARCHAR(36) NOT NULL,
  `rue` VARCHAR(45) NULL,
  `numero` VARCHAR(45) NULL,
  `ville` VARCHAR(45) NULL,
  `arrondissement` INT NULL,
  PRIMARY KEY (`adresseid`));
""")
conn.commit()

curs.execute("""
CREATE TABLE IF NOT EXISTS `geolocalisation` (
  `geolocalisationid` VARCHAR(36) NOT NULL,
  `latitude` REAL NOT NULL,
  `longitude` REAL NOT NULL,
  `adresseid` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`geolocalisationid`));
""")
conn.commit()

for index, row in df.iterrows():
    # pour chaque row, insérer en base
    adresseid = (requests.get(URL_ROOT + "/get_identifiant/adresse/osm" + urllib.parse.quote(str(row['N_rue']) + str(row['Rue']) + str(row['Ville']) + str(row['Arrondissement']) +str(row['Latitude_x']) + str(row['Longitude_y'])  ) )).text
    geolocalisationid =  (requests.get(URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(str(row['N_rue']) + str(row['Rue']) + str(row['Ville']) + str(row['Arrondissement']) +str(row['Latitude_x']) + str(row['Longitude_y'])  ) )).text

    curs.execute("insert into adresse (adresseid, numero, rue, ville, arrondissement) values(?,?,?,?,?)", (adresseid, row['N_rue'], row['Rue'], row['Ville'], row['Arrondissement']))
    if row['Latitude_x'] and row['Longitude_y']:
        curs.execute("insert into geolocalisation (geolocalisationid, adresseid, latitude, longitude) values(?,?,?,?)", (geolocalisationid, adresseid, row['Latitude_x'], row['Longitude_y']))

    conn.commit()

# charger le csv bano_75 venant de BANO dans une table de BASE_ADRESSES
curs.execute("""
CREATE TABLE IF NOT EXISTS `bano` (
  `rue` VARCHAR(100) NULL,
  `rue_norm` VARCHAR(100) NULL,
  `numero` VARCHAR(45) NULL,
  `arrondissement` INT NULL,
lat VARCHAR(40),
lon VARCHAR(40) );
""")
conn.commit()
import unidecode
df = pd.read_csv(CSV_BANO75, delimiter=",", header=None, usecols=[1,2,3,6,7])
df = df.replace({np.nan:None})
for index, row in df.iterrows():
    curs.execute("insert into bano (rue, rue_norm, numero, arrondissement, lat,lon) values(?,?,?,?,?,?)", (row[2], unidecode.unidecode(row[2].replace("-", " ").replace("'", " ").replace("/", " ")), row[1], int(re.sub(r"[^;]+;", "", str(row[3]).replace("750", "").replace("751", ""))), row[6], row[7]))
conn.commit()

# insérer les choix possibles dans la liste déroulante de l'IHM
curs.execute("""
CREATE TABLE IF NOT EXISTS `choix_ihm` (
    rue_init VARCHAR(100) not null, 
  `rue_norm` VARCHAR(100) not NULL ,
  rue_retournee VARCHAR(100) );
""")
conn.commit()
for a in RUES:
    curs.execute("insert into choix_ihm (rue_init, rue_norm, rue_retournee) values(?,?,?)", (a,unidecode.unidecode(a.replace("-", " ").replace("'", " ").replace("/", " ")), (re.sub(r"([^,]+), ?(.*)$", r"\2 \1", unidecode.unidecode(a.replace("-", " ").replace("'", " ").replace("/", " ")))).replace("' ", "'").replace("  ", " ")))
conn.commit()

# plutôt que de faire un alignement à partir des adresses rentrées dans l'inventaire, constituer directement une base de l'ensemble des adresses possibles avec la géoloc grâce à BANO: y a juste à aligner les noms de rues de l'ihm avec BANO
# aligner rues bano avec celles de l'IHM
curs.execute(""" create view if not exists rues_bano_to_ihm as with d_b as (select distinct rue, lower(rue_norm) as rue_norm from bano)
    select a.rue as rue_bano, b.rue_init as rue_ihm
    from d_b a 
    inner join choix_ihm b on lower(b.rue_retournee) = a.rue_norm""")
conn.commit()

# pour chaue adresse rentrée dans l'inventaire, chercher l'équivalent dans bano
curs.execute("""create view if not exists geoloc_inventaire_from_bano as select a.rue as rue_inventaire, a.numero as numero_inventaire , c.lat as lat_bano, c.lon as lon_bano
    from adresse a
    inner join rues_bano_to_ihm b on b.rue_ihm = a.rue 
    inner join bano c on c.rue = b.rue_bano and c.numero = a.numero
    where a.numero is not null and a.rue is not null""")
conn.commit()