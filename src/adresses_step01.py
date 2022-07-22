import os
import numpy as np
from shapely.geometry import Point, LineString
import geopandas as gpd
from shapely.geometry.polygon import Polygon
from shapely.ops import cascaded_union, unary_union
import dotenv
import re
import json
import requests
import sqlite3
import pandas as pd
import csv
import numpy as np
import urllib.parse
from utils.insert_geometries import Insert
from utils.extract_keywords import keywords
from utils.adresses import *


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))

DEBUG = os.environ['DEBUG']
KEY_WS = os.environ["KEY_WS"]
URL_ROOT  = os.environ["URL_ROOT"]
CSV_INVENTAIRE = os.environ["CSV_INVENTAIRE"]
BASE_ADRESSES = BASE_DIR + "/out/base_adresses.sqlite"
BASE_OSM = BASE_DIR + "/out/osm.sqlite"
BASE_INVENTAIRE = "/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite"

#obtenir les adresses distinctes avec leur éventuelle géoloc et les mettre dans nouvelle base
df = pd.read_csv(CSV_INVENTAIRE, delimiter="|",  usecols=['N_rue', 'Rue', 'Ville', 'Arrondissement', 'Latitude_x', 'Longitude_y'])
df  = df.drop_duplicates(['N_rue', 'Rue', 'Ville', 'Arrondissement'])
df = df.replace({np.nan:None})

if not (os.path.exists(BASE_ADRESSES)):
    open(BASE_ADRESSES, "w").close()    
conn = sqlite3.connect(BASE_ADRESSES)  
curs = conn.cursor()

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


# ETAPE 1bis:
# insérer les données de Paris
# convertir pbf file to csv osmconvert /home/maxime/Téléchargements/paris-latest.osm.pbf --csv="@id @oname @lat @lon addr:city addr:housename addr:housenumber addr:place addr:postcode addr:province addr:street addr:streetnumber building amenity bridge historic int_name int_ref landuse leisure loc_name loc_ref man_made military name nat_name nat_ref natural office official_name operator place postal_code ref shop short_name tourism waterway wikipedia wikidata" --csv-headline --csv-separator="|" -o=/home/maxime/dev/InventaireParisHistorique_automation/out/osm.csv
# puis convertir de csv en SQLITE : python3
# >> open("./osm.sqlite","w").close
# CREATE TABLE "data" ( "osmid"	INTEGER NOT NULL, "osmtype"	TEXT, "lat"	TEXT, lon TEXT NULL, "addr:city" TEXT NULL, "addr:housename" TEXT NULL, "addr:housenumber" TEXT NULL, "addr:place" TEXT NULL, "addr:postcode" TEXT NULL, "addr:province" TEXT NULL, "addr:street" TEXT NULL, "addr:streetnumber "TEXT NULL, building TEXT NULL, amenity TEXT NULL, bridge TEXT NULL, historic TEXT NULL, int_name TEXT NULL, int_ref TEXT NULL, landuse TEXT NULL, leisure TEXT NULL, loc_name TEXT NULL, loc_ref TEXT NULL, man_made TEXT NULL, military TEXT NULL, name TEXT NULL, nat_name TEXT NULL, nat_ref TEXT NULL, natural TEXT NULL, office TEXT NULL, official_name TEXT NULL, operator TEXT NULL, place TEXT NULL, postal_code TEXT NULL, ref TEXT NULL, shop TEXT NULL, short_name TEXT NULL, tourism TEXT NULL, waterway TEXT NULL, wikipedia TEXT NULL, wikidata TEXT NULL);
# CREATE INDEX "id_type" ON "data" ( "osmid"	ASC, "osmtype"	ASC)
# CREATE INDEX "osmid" ON "data" ( "osmid"	ASC )
# sqlite> .separator '|'
# sqlite> .import /home/maxime/dev/InventaireParisHistorique_automation/out/osm.csv data

request = """
    [out:json];
    rel(7444);
    out body;
    """
r = requests.get("https://overpass-api.de/api/interpreter?data="+urllib.parse.quote(request)).json()
# données
nom_fr = r["elements"][0]["tags"]["name:fr"]
nom_en = r["elements"][0]["tags"]["name"]
population = r["elements"][0]["tags"]["population"]
wikidata = r["elements"][0]["tags"]["wikidata"]
insee = r["elements"][0]["tags"]["ref:INSEE"]
# pour chaque membre de type outer, avoir les coordonnées pour créer les coordinates de Paris
acumulation_list = []
conn_osm = sqlite3.connect(BASE_OSM)  
curs_osm = conn_osm.cursor()
for el in r["elements"][0]["members"]:
    if el["role"] == "outer": 
        print("way: " +str(el["ref"]))
        # il me faut tous les noeuds de ce way:
        if el["type"] != "node":
            request2 = "[out:json];"+str(el["type"])+"("+str(el["ref"])+");out body;"
            try:
                nodes = (requests.get("https://overpass-api.de/api/interpreter?data="+urllib.parse.quote(request2)).json())["elements"][0]["nodes"]
                for node in nodes:
                    lat, lon = curs_osm.execute("select lat, lon from data where osmtype = '"+str("node")+"' and osmid = " + str(node)).fetchall()[0]
            except Exception as e:
                print(e)
                lat, lon = (None, None)
        else:
            lat, lon = curs_osm.execute("select lat, lon from data where osmtype = '"+str(el["type"])+"' and osmid = " + str(el["ref"])).fetchall()[0]
        if lat and lon:
            acumulation_list.append([float(lat), float(lon)])
conn_osm.close()
curs.execute("""
CREATE TABLE IF NOT EXISTS `geometry` (
  `geometryid` VARCHAR(36) NOT NULL,
  `geometrytype` VARCHAR(20) NOT NULL,
  geometryareatype	TEXT,
  PRIMARY KEY (`geometryid`));
""")
conn.commit()
curs.execute("""
CREATE TABLE IF NOT EXISTS `geometry_text` (
  `geometryid` VARCHAR(36) NOT NULL,
  `textid` VARCHAR(36) NOT NULL,
  textvalue TEXT NULL,
  textlang VARCHAR(36) null,
  relationtype VARCHAR(36),
  PRIMARY KEY (`textid`));
""")
conn.commit()
curs.execute("""
CREATE TABLE IF NOT EXISTS `geometry_identifier` (
  `geometryid` VARCHAR(36) NOT NULL,
  `identifierid` VARCHAR(36) NOT NULL,
  `identifiertype` VARCHAR(36) NULL,
  identifiervalue TEXT NULL,
  relationtype VARCHAR(36),
  PRIMARY KEY (`identifierid`));
""")
conn.commit()
curs.execute("""
CREATE TABLE IF NOT EXISTS `geometry_concept` (
  `geometryid` VARCHAR(36) NOT NULL,
  `conceptid` VARCHAR(36) NOT NULL,
  relationtype VARCHAR(36));
""")
conn.commit()
curs.execute("""
CREATE TABLE IF NOT EXISTS `geometry_geolocalisation` (
  `geolocalisationid` VARCHAR(36) NOT NULL,
  `latitude` REAL NULL,
  `longitude` REAL  NULL,
  `geometryid` VARCHAR(45) NOT NULL,
  polygone TEXT null,
  PRIMARY KEY (`geolocalisationid`))
;
""")
conn.commit()
geometryid = (requests.get(URL_ROOT + "/get_identifiant/geometry/osm" + urllib.parse.quote(nom_fr + wikidata  ) )).text
curs.execute("insert into geometry values ('"+str(geometryid)+"', 'relation', 'VILLE')")
curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + nom_fr  ) )).text+"', '"+str(geometryid)+"', '"+nom_fr.replace("'", "''")+"','LIBELLE', 'fre')")
curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + nom_en + "anglais"  ) )).text+"', '"+str(geometryid)+"', '"+nom_en.replace("'", "''")+"','LIBELLE', 'eng')")
curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + population ) )).text+"', '"+str(geometryid)+"', '"+str(population).replace("'", "''")+"','POPULATION', 'fre')")
curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + wikidata ) )).text+"', 'WIKIDATA', 'IDEXT', '"+wikidata+"')")
curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + insee ) )).text+"', 'INSEE', 'IDEXT', '"+insee+"')")
curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + '7444') )).text+"', 'OSMID', 'IDEXT', '7444')")
curs.execute("insert into geometry_geolocalisation(geometryid, geolocalisationid, polygone) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(geometryid + str(acumulation_list) ) )).text+"', '"+str(acumulation_list)+"')")
conn.commit()

# insérer les données des arrondissments de Paris

request = """
    [out:json];
    rel(7444);
    rel(r:"subarea");
    out body;
    """
r = requests.get("https://overpass-api.de/api/interpreter?data="+urllib.parse.quote(request)).json()
for el in r["elements"]:
    # traiter un arrondissement à la fois
    nom_fr = el["tags"]["name:fr"]
    nom_en = el["tags"]["name"]
    population = el["tags"]["population"]
    wikidata = el["tags"]["wikidata"]
    insee = el["tags"]["ref:INSEE"]
    postcode = el["tags"]["postal_code"]
    osmtype = el["type"]
    osmid = el["id"]
    acumulation_list = []
    conn_osm = sqlite3.connect(BASE_OSM)  
    curs_osm = conn_osm.cursor()
    print(str(osmid) + " : " + str(postcode))
    for m in el["members"]:
        if m["role"] == "outer": 
            print("way: " +str(m["ref"]))
            # il me faut tous les noeuds de ce way:
            if m["type"] != "node":
                request2 = "[out:json];"+str(m["type"])+"("+str(m["ref"])+");out body;"
                try:
                    nodes = (requests.get("https://overpass-api.de/api/interpreter?data="+urllib.parse.quote(request2)).json())["elements"][0]["nodes"]
                    for node in nodes:
                        lat, lon = curs_osm.execute("select lat, lon from data where osmtype = '"+str("node")+"' and osmid = " + str(node)).fetchall()[0]
                except Exception as e:
                    print(e)
                    lat, lon = (None, None)
            else:
                lat, lon = curs_osm.execute("select lat, lon from data where osmtype = '"+str(m["type"])+"' and osmid = " + str(m["ref"])).fetchall()[0]
            if lat and lon:
                acumulation_list.append([float(lat), float(lon)])
    conn_osm.close()
    curs.execute("""
        CREATE TABLE IF NOT EXISTS `geometry_geometry` (
        `sourcegeometryid` VARCHAR(36) NOT NULL,
        `targetgeometryid` VARCHAR(36) NOT NULL,
        `relationtype` VARCHAR(36)  NULL);
    """)
    conn.commit()
    geometryid = (requests.get(URL_ROOT + "/get_identifiant/geometry/osm" + urllib.parse.quote(nom_fr + wikidata  ) )).text
    curs.execute("insert into geometry values ('"+str(geometryid)+"', '"+str(osmtype)+"', 'ARRONDISSEMENT')")
    curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + nom_fr  ) )).text+"', '"+str(geometryid)+"', '"+nom_fr.replace("'", "''")+"','LIBELLE', 'fre')")
    curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + str(postcode)  ) )).text+"', '"+str(geometryid)+"', '"+str(postcode).replace("'", "''")+"','CODE_POSTAL', 'fre')")
    curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + nom_en + "anglais"  ) )).text+"', '"+str(geometryid)+"', '"+nom_en.replace("'", "''")+"','LIBELLE', 'eng')")
    curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + population ) )).text+"', '"+str(geometryid)+"', '"+str(population).replace("'", "''")+"','POPULATION', 'fre')")
    curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + wikidata ) )).text+"', 'WIKIDATA', 'IDEXT', '"+wikidata+"')")
    curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + str(osmid)) )).text+"', 'OSMID', 'IDEXT', '"+str(osmid)+"')")
    curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + insee ) )).text+"', 'INSEE', 'IDEXT', '"+insee+"')")
    curs.execute("insert into geometry_geolocalisation(geometryid, geolocalisationid, polygone) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(geometryid + str(acumulation_list) ) )).text+"', '"+str(acumulation_list)+"')")
    curs.execute("insert into geometry_geometry values ('"+str(curs.execute("select geometryid from geometry_identifier where relationtype = 'WIKIDATA' and identifiervalue = 'Q90'").fetchall()[0][0])+"', '"+geometryid+"', 'PARENT_DE')")
    conn.commit()

# insérer les quartiers

request = """
    [out:json];
    rel[boundary="administrative"]["addr:postcode"~"^75.*"];
    out body;
    """
r = requests.get("https://overpass-api.de/api/interpreter?data="+urllib.parse.quote(request)).json()
for el in r["elements"]:
    nom_fr = el["tags"]["name"]
    if "wikidata" in el["tags"]:
        wikidata = el["tags"]["wikidata"]
    else:
        wikidata = None
    postcode = el["tags"]["addr:postcode"]
    geometryid_arrdsmt= str(curs.execute("select geometryid from geometry_text where relationtype = 'CODE_POSTAL' and textvalue like '"+str(postcode)+"%'").fetchall()[0][0])
    osmtype = el["type"]
    osmid = el["id"]
    acumulation_list = []
    conn_osm = sqlite3.connect(BASE_OSM)  
    curs_osm = conn_osm.cursor()
    print(str(osmid) + " : " + str(postcode))
    for m in el["members"]:
        if m["role"] == "outer": 
            print("way: " +str(m["ref"]))
            # il me faut tous les noeuds de ce way:
            if m["type"] != "node":
                request2 = "[out:json];"+str(m["type"])+"("+str(m["ref"])+");out body;"
                try:
                    nodes = (requests.get("https://overpass-api.de/api/interpreter?data="+urllib.parse.quote(request2)).json())["elements"][0]["nodes"]
                    for node in nodes:
                        lat, lon = curs_osm.execute("select lat, lon from data where osmtype = '"+str("node")+"' and osmid = " + str(node)).fetchall()[0]
                except Exception as e:
                    print(e)
                    lat, lon = (None, None)
            else:
                lat, lon = curs_osm.execute("select lat, lon from data where osmtype = '"+str(m["type"])+"' and osmid = " + str(m["ref"])).fetchall()[0]
            if lat and lon:
                acumulation_list.append([float(lat), float(lon)])
    conn_osm.close()
    geometryid = (requests.get(URL_ROOT + "/get_identifiant/geometry/osm" + urllib.parse.quote(nom_fr + wikidata  ) )).text
    curs.execute("insert into geometry values ('"+str(geometryid)+"', '"+str(osmtype)+"','QUARTIER')")
    curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + nom_fr  ) )).text+"', '"+str(geometryid)+"', '"+nom_fr.replace("'", "''")+"','LIBELLE', 'fre')")
    curs.execute("insert into geometry_text (textid, geometryid,  textvalue, relationtype, textlang) values ('"+(requests.get(URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + str(postcode)  ) )).text+"', '"+str(geometryid)+"', '"+str(postcode).replace("'", "''")+"','CODE_POSTAL', 'fre')")
    if wikidata:
        curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + wikidata ) )).text+"', 'WIKIDATA', 'IDEXT', '"+wikidata+"')")
    curs.execute("insert into geometry_identifier (geometryid, identifierid, relationtype, identifiertype, identifiervalue) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + str(osmid)) )).text+"', 'OSMID', 'IDEXT', '"+str(osmid)+"')")
    curs.execute("insert into geometry_geolocalisation(geometryid, geolocalisationid, polygone) values ('"+geometryid+"', '"+(requests.get(URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(geometryid + str(acumulation_list) ) )).text+"', '"+str(acumulation_list)+"')")
    curs.execute("insert into geometry_geometry values ('"+geometryid_arrdsmt+"', '"+geometryid+"', 'PARENT_DE')")
    conn.commit()

conn.close()