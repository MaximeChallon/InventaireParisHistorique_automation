# le but de ce script est de créer une base de données à partir des adresses rentrées dans l'inventaire afin de les aligner avec OSM et d'en tirer le maximum de descripteurs et de données

# 1- passer les adresses distinctes dans cette base
# 1 bis: importer en base les données de Prais, des arrdsmts et des quartiers
# 2 - pour chaque adresse, appeler le service de recherche inversée à partir de lat long ou de recherche normale à partir adresse
# 3 - pour chaque noeud, insérer en base avec ses attributs, chercher ses noeuds et ways alentours dans lequel il est compris: utiliser le service dédié
# 4 - insérer en base les données obtenues

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

def object_has_childs(base_adresses, type_object, id_object):
    has = False
    conn = sqlite3.connect(base_adresses)  
    curs = conn.cursor()
    if len(curs.execute("select * from geometry_geometry inner join geometry on targetgeometryid = geometryid and geometrytype = '"+type_object+"' where targetgeometryid = '"+id_object+"'").fetchall()) > 0:
        has = True
    conn.close()
    return has


def is_in_geometry(lat, lon, lons_vect,lats_vect, geometryid_vect):
    area_matched = []
    area_around = []
    for osmobject in geometryid_vect:
        if lat and lon:
            if Polygon(np.column_stack((np.array(lons_vect[geometryid_vect.index(osmobject)]), np.array(lats_vect[geometryid_vect.index(osmobject)])))).contains(Point(float(lon),float(lat))):
                area_matched.append(osmobject)
            else:
                pass
                # area_around.append(osmobject)
    return (area_matched, area_around)

def object_exists(base_adresses, id):
    exists=False
    conn = sqlite3.connect(base_adresses)  
    curs = conn.cursor()
    if len(curs.execute("select * from geometry where geometryid = '"+id+"'").fetchall()) > 0:
        exists = True
    conn.close()
    return exists

def get_geometryid_from_osmid(osmid, osmtype, areatype, curs, URL_ROOT):
    deja_present = curs.execute("select geometryid from geometry_identifier where identifiervalue = '"+str(osmid)+"' and identifiertype = 'IDEXT' and relationtype = 'OSMID'").fetchall()
    if len(deja_present) > 0:
        geometryid = deja_present[0][0]
    else:
        geometryid = (requests.get(URL_ROOT + "/get_identifiant/geometry/osm" + urllib.parse.quote(str(osmid) + areatype + osmtype  ) )).text
    return geometryid

# ETAPE 1
'''
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

'''
# ETAPE 1bis:
# insérer les données de Paris
# convertir pbf file to csv osmconvert /home/maxime/Téléchargements/paris-latest.osm.pbf --csv="@id @oname @lat @lon addr:city addr:housename addr:housenumber addr:place addr:postcode addr:province addr:street addr:streetnumber building amenity bridge historic int_name int_ref landuse leisure loc_name loc_ref man_made military name nat_name nat_ref natural office official_name operator place postal_code ref shop short_name tourism waterway wikipedia wikidata" --csv-headline --csv-separator="|" -o=/home/maxime/dev/InventaireParisHistorique_automation/out/osm.csv
# puis convertir de csv en SQLITE 
# CREATE TABLE "data" ( "osmid"	INTEGER NOT NULL, "osmtype"	TEXT, "lat"	TEXT, lon TEXT NULL, "addr:city" TEXT NULL, "addr:housename" TEXT NULL, "addr:housenumber" TEXT NULL, "addr:place" TEXT NULL, "addr:postcode" TEXT NULL, "addr:province" TEXT NULL, "addr:street" TEXT NULL, "addr:streetnumber "TEXT NULL, building TEXT NULL, amenity TEXT NULL, bridge TEXT NULL, historic TEXT NULL, int_name TEXT NULL, int_ref TEXT NULL, landuse TEXT NULL, leisure TEXT NULL, loc_name TEXT NULL, loc_ref TEXT NULL, man_made TEXT NULL, military TEXT NULL, name TEXT NULL, nat_name TEXT NULL, nat_ref TEXT NULL, natural TEXT NULL, office TEXT NULL, official_name TEXT NULL, operator TEXT NULL, place TEXT NULL, postal_code TEXT NULL, ref TEXT NULL, shop TEXT NULL, short_name TEXT NULL, tourism TEXT NULL, waterway TEXT NULL, wikipedia TEXT NULL, wikidata TEXT NULL);
# CREATE INDEX "id_type" ON "data" ( "osmid"	ASC, "osmtype"	ASC)
# CREATE INDEX "osmid" ON "data" ( "osmid"	ASC )
# sqlite> .separator '|' 
# sqlite> .import /home/maxime/dev/InventaireParisHistorique_automation/out/osm.csv data
'''
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
'''

# insérer les données des arrondissments de Paris
'''
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

'''

# ETAPE 2:
# - chercher dans osm par l'adresse, si trouve rien, alors j'utilise la géoloc stockée dans inventaire
# je charge d'abord les objets quartiers, arrdsmt et paris et leur lats_vect et lons_vect
conn_osm = sqlite3.connect(BASE_OSM)  
curs_osm = conn_osm.cursor()
lats_vect = []
lons_vect = []
geometryid_vect = []
for o in curs.execute("select a.geometryid, a.polygone from geometry_geolocalisation a inner join geometry b on b.geometryid = a.geometryid where b.geometryareatype in ('VILLE', 'ARRONDISSEMENT', 'QUARTIER')").fetchall():
    tmp_lats = []
    tmp_lons = []
    for couple in o[1].replace("], [", "@").strip("][").split("@"):
        c = couple.replace(" ", "").split(",")
        if len(c) == 2:
            tmp_lats.append(float(c[0]))
            tmp_lons.append(float(c[1]))
    if tmp_lons and tmp_lats and len(tmp_lons) > 2 and len(tmp_lats) > 2:
        lats_vect.append(tmp_lats)
        lons_vect.append(tmp_lons)
        geometryid_vect.append(o[0])

curs.execute("create table if not exists adresse_geometry (adresseid varchar(36) not null, geometryid varchar(36) not null, indice integer);")
conn.commit()

curs.execute("create table if not exists geometry_concept (geometryid varchar(36) not null, conceptid varchar(36) not null, relationtype varchar(36) null);")
conn.commit()

for r in curs.execute("select * from adresse left join geolocalisation on geolocalisation.adresseid = adresse.adresseid left join adresse_geometry on adresse_geometry.adresseid = adresse.adresseid where adresse_geometry.geometryid  is null and geolocalisation.relationtype = 'COORDONNEES_INVENTAIRE'  and adresse.rue is not null  and adresse.rue not like '48%' order by adresse.rue limit 500 ").fetchall():
    print(r)
    results = curs_osm.execute("select \"addr:housenumber\",\"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where \"addr:street\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', str(r[1]))).lower().replace(" ", "%").replace("'", "''") if r[1] else None)+"' " + ("and \"addr:housenumber\" = '"+str(r[2])+"'"if r[2] else " and osmtype = 'way' and \"addr:housenumber\" is null")).fetchall()
    if not results:
        results = []
        # essayer de rechercher en scindant le num de la rue
        if r[2] and re.match(r'[0-9]+[^0-9]+[0-9]+.*', r[2]):
            ns = re.split(r'[^0-9]+', re.sub(r'[^0-9]*$', '',r[2]))
            i = int(ns[0])
            for n in ns:
                if int(i) <= int(ns[-1]):
                    result = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where \"addr:street\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', str(r[1]))).lower().replace(" ", "%").replace("'", "''") if r[1] else None)+"' " + ("and \"addr:housenumber\" = '"+str(n)+"'"if n else " and osmtype = 'way' and \"addr:housenumber\" is null")).fetchall()
                    for _r in result:
                        results.append(_r)
                    i += 2
        # essayer de rechercher si c'est une rue et si elle existe dans osm
        if not r[2]:
            result = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where \"name\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', str(r[1]))).lower().replace(" ", "%").replace("'", "''") if r[1] else None)+"' " ).fetchall()
            for _result in result:
                # créer l'objet relation
                relationid = get_geometryid_from_osmid(_result[2], 'relation', 'VOIE', curs, URL_ROOT)
                insert_relation = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                insert_relation.geometry('relation', 'VOIE', _result[2], relationid)
                insert_relation.geometry_identifier(_result[2], 'IDEXT', 'OSMID', relationid)
                insert_relation.geometry_identifier(_result[6], 'IDEXT', 'WIKIDATA', relationid)
                insert_relation.geometry_text(_result[5], "fre", "LIBELLE", relationid)
                insert_relation.adresse_geometry(r[0], relationid, 90)
                insert_relation.geometry_concept(relationid, keywords("").from_base_osm(BASE_OSM, _result[2]), BASE_INVENTAIRE)
                # chercher ses ways
                relation = json.loads((requests.get(URL_ROOT + "/get_ways/relation/"+str(_result[2]) )).text)["elements"][0]
                polygone_relation = []
                if not object_has_childs(BASE_ADRESSES, 'relation', relationid):
                    for way in relation["members"]:
                        # je ne garde que ce qui ne concerne que la rue
                        wayid = get_geometryid_from_osmid(way["ref"], way["type"], 'VOIE', curs, URL_ROOT)
                        if way["type"] == "way" and (not way["role"] or way["role"] == 'street') and not object_exists(BASE_ADRESSES, wayid):
                            result_way = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where osmid = '"+str(way["ref"])+"'" ).fetchall()[0]
                            insert_way = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                            insert_way.geometry('way', 'VOIE', result_way[2], wayid)
                            insert_way.geometry_identifier(result_way[2], 'IDEXT', 'OSMID', wayid)
                            insert_way.geometry_identifier(result_way[6], 'IDEXT', 'WIKIDATA', wayid)
                            insert_way.geometry_text(result_way[5], "fre", "LIBELLE", wayid)
                            insert_way.geometry_geometry(wayid, relationid, 'COMPRIS DANS')
                            insert_way.geometry_concept(wayid, keywords("").from_base_osm(BASE_OSM, result_way[2]), BASE_INVENTAIRE)
                            # chercher les noeuds des ways pour avoir polygone
                            noeuds_way = json.loads((requests.get(URL_ROOT + "/get_nodes/way/"+str(result_way[2]) )).text)
                            polygone_way_o = []
                            if not object_has_childs(BASE_ADRESSES, 'way', wayid):
                                for noeud_way in noeuds_way["elements"]:
                                    type_noeud_way = noeud_way["type"]
                                    id_noeud_way = noeud_way["id"]
                                    noeuds_way_osm = noeud_way["nodes"]
                                    contenu_dans_way = []
                                    for n_way_osm in noeuds_way_osm:
                                        result_way_osm = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where  osmtype = 'node' and osmid = '"+str(n_way_osm)+"'").fetchall()
                                        for rr_way_osm in result_way_osm:
                                            if rr_way_osm[3] and rr_way_osm[4]:
                                                polygone_way_o.append([float(rr_way_osm[3]), float(rr_way_osm[4])])
                                                noid_way = get_geometryid_from_osmid(rr_way_osm[2], 'node', 'POINT', curs, URL_ROOT)
                                                insert_node_osm_way = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                                                insert_node_osm_way.geometry('node', 'POINT', str(rr_way_osm[2]), noid_way)
                                                insert_node_osm_way.geometry_identifier(str(rr_way_osm[2]), 'IDEXT', 'OSMID', noid_way)
                                                if rr_way_osm[6]:
                                                    insert_node_osm_way.geometry_identifier(str(rr_way_osm[6]), 'IDEXT', 'WIKIDATA', noid_way)
                                                if rr_way_osm[5]:
                                                    insert_node_osm_way.geometry_text(str(rr_way_osm[5]), "fre", "LIBELLE", noid_way)
                                                insert_node_osm_way.geometry_geometry(noid_way, wayid, 'COMPRIS DANS')
                                                insert_node_osm_way.geometry_concept(noid_way, keywords("").from_base_osm(BASE_OSM, rr_way_osm[2]), BASE_INVENTAIRE)
                                                insert_node_osm_way.geometry_geolocalisation(geometryid=noid_way, lat=rr_way_osm[3], lon=rr_way_osm[4], polygone=None)
                                                area_matched5, area_around5 = is_in_geometry(rr_way_osm[3], rr_way_osm[4], lons_vect, lats_vect, geometryid_vect)
                                                for a5 in area_matched5:
                                                    if a5 not in contenu_dans_way:
                                                        contenu_dans_way.append(a5)
                                    for area5 in contenu_dans_way:
                                        insert_way.geometry_geometry(wayid, area5, 'COMPRIS DANS')        
                            #polygone du way
                            polygone_relation.append(polygone_way_o)
                            insert_way.geometry_geolocalisation(geometryid=wayid, lat=None, lon=None, polygone=polygone_way_o)   
                    # créer le polygone de l'objet relation
                    insert_relation.geometry_geolocalisation(geometryid=relationid, lat=None, lon=None, polygone=polygone_relation) 

    if results:
        for r_ in results:
            area_matched, area_around = is_in_geometry(r_[3], r_[4], lons_vect, lats_vect, geometryid_vect)
            # insertion en base
            geometryid = get_geometryid_from_osmid(r_[2], r_[7], 'BATIMENT', curs, URL_ROOT)
            insert = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
            insert.geometry(r_[7], 'BATIMENT', r_[2], geometryid)
            insert.geometry_identifier(r_[2], 'IDEXT', 'OSMID', geometryid)
            insert.geometry_identifier(r_[6], 'IDEXT', 'WIKIDATA', geometryid)
            insert.geometry_text(r_[5], "fre", "LIBELLE", geometryid)
            if r_[7] == "node":
                if r_[3] and r_[4]:
                    insert.geometry_geolocalisation(geometryid=geometryid, lat=r_[3], lon=r_[4], polygone=None)
            else:
                noeuds = json.loads((requests.get(URL_ROOT + "/get_nodes/"+str(r_[7])+"/"+str(r_[2]) )).text)
                polygone_o = []
                for noeud in noeuds["elements"]:
                    type_noeud = noeud["type"]
                    id_noeud = noeud["id"]
                    if "nodes" in noeud:
                        noeuds_osm = noeud["nodes"]
                        contenu_dans = []
                        for n_osm in noeuds_osm:
                            result_osm = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where  osmtype = 'node' and osmid = '"+str(n_osm)+"'").fetchall()
                            for rr_osm in result_osm:
                                if rr_osm[3] and rr_osm[4]:
                                    polygone_o.append([float(rr_osm[3]), float(rr_osm[4])])
                                    noid = get_geometryid_from_osmid(rr_osm[2], 'node', 'POINT', curs, URL_ROOT)
                                    insert_node_osm = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                                    insert_node_osm.geometry('node', 'POINT', str(rr_osm[2]), noid)
                                    insert_node_osm.geometry_identifier(str(rr_osm[2]), 'IDEXT', 'OSMID', noid)
                                    if rr_osm[6]:
                                        insert_node_osm.geometry_identifier(str(rr_osm[6]), 'IDEXT', 'WIKIDATA', noid)
                                    if rr_osm[5]:
                                        insert_node_osm.geometry_text(str(rr_osm[5]), "fre", "LIBELLE", noid)
                                    insert_node_osm.geometry_geometry(noid, geometryid, 'COMPRIS DANS')
                                    insert_node_osm.geometry_concept(noid, keywords("").from_base_osm(BASE_OSM, rr_osm[2]), BASE_INVENTAIRE)
                                    insert_node_osm.geometry_geolocalisation(geometryid=noid, lat=rr_osm[3], lon=rr_osm[4], polygone=None)
                                    area_matched2, area_around2 = is_in_geometry(rr_osm[3], rr_osm[4], lons_vect, lats_vect, geometryid_vect)
                                    for a in area_matched2:
                                        if a not in contenu_dans:
                                            contenu_dans.append(a)
                        for area2 in contenu_dans:
                            insert.geometry_geometry(geometryid, area2, 'COMPRIS DANS')        
                insert.geometry_geolocalisation(geometryid=geometryid, lat=None, lon=None, polygone=polygone_o)    
            for area in area_matched:
                insert.geometry_geometry(geometryid, area, 'COMPRIS DANS')
            for area in area_around:
                insert.geometry_geometry(geometryid, area, 'PROCHE DE')
            insert.adresse_geometry(r[0], geometryid, 100)
            insert.geometry_concept(geometryid, keywords("").from_base_osm(BASE_OSM, r_[2]), BASE_INVENTAIRE)
    elif not results and r[2]:
        # recherche par around si geoloc chez moi
        if r[6] and r[7]:
            # je cherche les ways proches
            around = json.loads((requests.get(URL_ROOT + "/get_around_objects/10/"+str(r[6])+"/"+str(r[7]) )).text)
            for element in around["elements"]:
                type_objet = element["type"]
                id_objet = element["id"]
                nodes = element["nodes"]
                lat_array = []
                lon_array = []
                proches = []
                in_objects = []
                polygone_objet = []
                # insertion du way et de ses attributs
                wayid = get_geometryid_from_osmid(id_objet, 'way', 'BATIMENT', curs, URL_ROOT)
                insert_way = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                insert_way.geometry(type_objet, 'BATIMENT', id_objet, wayid)
                insert_way.geometry_identifier(id_objet, 'IDEXT', 'OSMID', wayid)
                if "tags" in element:
                    if "wikidata" in element["tags"]:
                        insert_way.geometry_identifier(element["tags"]["wikidata"], 'IDEXT', 'WIKIDATA', wayid)
                    if "name" in element["tags"]:
                        insert_way.geometry_text(element["tags"]["name"], "fre", "LIBELLE", wayid)
                # traiteent des nodes
                for node in nodes:
                    result = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where  osmtype = 'node' and osmid = '"+str(node)+"'").fetchall()
                    contenu_dans2 = []
                    for rr in result:
                        if rr[3] and rr[4]:
                            lat_array.append(rr[3])
                            lon_array.append(rr[4])
                            polygone_objet.append([float(rr[3]), float(rr[4])])
                            nodeid = get_geometryid_from_osmid(rr[2], 'node', 'POINT', curs, URL_ROOT)
                            insert_node = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                            insert_node.geometry(type_objet, 'POINT', str(rr[2]), nodeid)
                            insert_node.geometry_identifier(str(rr[2]), 'IDEXT', 'OSMID', nodeid)
                            if rr[6]:
                                insert_node.geometry_identifier(str(rr[6]), 'IDEXT', 'WIKIDATA', nodeid)
                            if rr[5]:
                                insert_node.geometry_text(str(rr[5]), "fre", "LIBELLE", nodeid)
                            insert_node.geometry_geometry(nodeid, wayid, 'COMPRIS DANS')
                            insert_node.geometry_concept(nodeid, keywords("").from_base_osm(BASE_OSM, rr[2]), BASE_INVENTAIRE)
                            insert_node.geometry_geolocalisation(geometryid=nodeid, lat=rr[3], lon=rr[4], polygone=None)
                            area_matched3, area_around3 = is_in_geometry(rr[3], rr[4], lons_vect, lats_vect, geometryid_vect)
                            for a2 in area_matched3:
                                if a2 not in contenu_dans2:
                                    contenu_dans2.append(a2)
                    for area3 in contenu_dans2:
                        insert_way.geometry_geometry(wayid, area3, 'COMPRIS DANS')
                if len(lon_array) > 2 and Polygon(np.column_stack((np.array(lon_array), np.array(lat_array)))).contains(Point(float(r[7]),float(r[6]))):
                    in_objects.append(id_objet)
                else:
                    proches.append(id_objet) 
                insert_way.geometry_geolocalisation(geometryid=wayid, lat=None, lon=None, polygone=polygone_objet)
                for area in in_objects:
                    insert_way.geometry_geometry(wayid, curs.execute("select geometryid from geometry_identifier where identifiertype = 'IDEXT' and relationtype = 'OSMID' and identifiervalue = '"+str(area)+"'").fetchall()[0][0], 'COMPRIS DANS')
                for area_ in proches:
                    insert_way.geometry_geometry(wayid, curs.execute("select geometryid from geometry_identifier where identifiertype = 'IDEXT' and relationtype = 'OSMID' and identifiervalue = '"+str(area_)+"'").fetchall()[0][0], 'PROCHE DE')
                insert_way.adresse_geometry(r[0], wayid,40)
                insert_way.geometry_concept(wayid, keywords("").from_base_osm(BASE_OSM, str(id_objet)), BASE_INVENTAIRE)
            # je peux ensuite chercher les nodes proches:
            around_nodes = json.loads((requests.get(URL_ROOT + "/get_around_nodes/10/"+str(r[6])+"/"+str(r[7]) )).text)
            for el in around_nodes["elements"]:
                node_id = el["id"]
                node_type = el["type"]
                node_lat = el["lat"]
                node_lon = el["lon"]
                node_housenumber = el["tags"]["addr:housenumber"]
                proches_ = []
                in_objects_ = []
                result = curs_osm.execute("select \"addr:housenumber\", \"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where  osmtype = 'node' and osmid = '"+str(node_id)+"'").fetchall()
                if str(node_housenumber) == str(r[2]):
                    in_objects_.append(node_id)
                else:
                    proches_.append(node_id)
                id = get_geometryid_from_osmid(node_id, 'node', 'POINT', curs, URL_ROOT)
                insert_n = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
                insert_n.geometry(node_type, 'POINT', str(node_id), id)
                insert_n.geometry_identifier(str(node_id), 'IDEXT', 'OSMID', id)
                if "tags" in el:
                    if "wikidata" in el["tags"]:
                        insert_n.geometry_identifier(str(el["tags"]["wikidata"]), 'IDEXT', 'WIKIDATA', id)
                    if "name" in el["tags"]:
                        insert_n.geometry_text(str(el["tags"]["name"]), "fre", "LIBELLE", id)
                for i in in_objects_:
                    insert_way.adresse_geometry(r[0], curs.execute("select geometryid from geometry_identifier where identifiervalue = '"+str(i)+"' and identifiertype = 'IDEXT' and relationtype = 'OSMID'").fetchall()[0][0], 40)
                insert_n.geometry_concept(id, keywords("").from_base_osm(BASE_OSM, node_id), BASE_INVENTAIRE)
                insert_n.geometry_geolocalisation(geometryid=id, lat=node_lat, lon=node_lon, polygone=None)
                # TODO: checkker dans quel quartier c'est 
                area_matched4, area_around4 = is_in_geometry(node_lat, node_lon, lons_vect, lats_vect, geometryid_vect)
                for area4 in area_matched4:
                    insert_n.geometry_geometry(id, area4, 'COMPRIS DANS') 

conn_osm.close()



'''
# ETAPE 3: pour chaque adresse qui a une geometry associée de type polygone, calculer un seul point pour l'associer à cette adresse pour le pointeur leaflet: dans tous les cas (polygone ou latloon), créer une géolocalisation associe à l'adresse flaguée osm

# avoir un seul polygone à partir de plusieurs
def merge_multi_polygons(multi_polygons):
    # pour merger ensemble des polygones qui ont au moins un chevauchement
    polygons = []
    for poly in multi_polygons:
        lats_v = []
        lons_v = []
        for p in poly:
            lats_v.append(p[0])
            lons_v.append(p[1])
        polygons.append(Polygon(np.column_stack((np.array(lons_v), np.array(lats_v)))))
    unary_union(polygons)
    # TODO: transformer polygons en liste de points pour leaflet
    return polygons

# pour assembler des lignes en un seul polygone
def multi_polygons_to_line_polygon(multi_polygons, type_multi_polygones):
    points = []
    if type_multi_polygones == 'relation':
        for poly in multi_polygons.replace('[[[', '[[').replace(']]]', ']]').replace(']], [', ']]@[').split('@'):
            for p in poly.replace('[[', '').replace(']]', '').split('], ['):
                points.append(Point(float(p.split(', ')[1]), float(p.split(', ')[0])))
    polygone_final = []
    for point in LineString(points).coords[:]:
        polygone_final.append([point[1], point[0]])
    if not polygone_final:
        return multi_polygons
    return polygone_final

# créer un eul polygone pour lesobjets faits de plusieurs

for result in curs.execute("""select a.adresseid, a.geometryid, b.polygone, b.geolocalisationid, c.geometrytype
from adresse_geometry a
inner join geometry_geolocalisation b on b.geometryid = a.geometryid and b.polygone like '[[[%'
inner join geometry c on c.geometryid = a.geometryid""").fetchall():
    new = multi_polygons_to_line_polygon(result[2],result[4])
    update_geoloc = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
    update_geoloc.update_geometry_geolocalisation(None, None, result[3], result[1],new)

# ajout colonne type dans adresse_geolocalisation
try:
    curs.execute("alter table geolocalisation add column relationtype varchar(36) null ")
except:
    pass
curs.execute("update geolocalisation set relationtype = 'COORDONNEES_INVENTAIRE' where relationtype is null")
conn.commit()
# calculer le centre des polygones
for result in curs.execute("""with rangs as (select a.adresseid, a.geometryid , rank() over(partition by a.adresseid, a.indice, case when  c.geometrytype = 'relation' then 1
	when c.geometrytype = 'way' then 2 else 3 end ,case when  c.geometryareatype = 'BATIMENT' then 1
	when c.geometryareatype = 'VOIE' then 2 else 3 end    order by a.adresseid, a.geometryid, a.indice desc, case when  c.geometrytype = 'relation' then 1
	when c.geometrytype = 'way' then 2 else 3 end ,case when  c.geometryareatype = 'BATIMENT' then 1
	when c.geometryareatype = 'VOIE' then 2 else 3 end ) as rang
from adresse_geometry a
inner join geometry_geolocalisation b on b.geometryid = a.geometryid and b.polygone is not null
inner join geometry c on c.geometryid = a.geometryid 
group by a.adresseid, a.geometryid)
select a.adresseid, a.geometryid, b.polygone
from rangs a
inner join geometry_geolocalisation b on b.geometryid = a.geometryid
left join geolocalisation c on c.adresseid = a.adresseid and c.relationtype = 'REF_LEAFLET'
where a.rang = 1 and c.geolocalisationid is null""").fetchall():
    polygone = result[2]
    lats_vecteur = []
    lons_vecteur = []
    for p in polygone.replace('[[', '').replace(']]', '').split('], ['):
        lons_vecteur.append(float(p.split(', ')[1]))
        lats_vecteur.append(float(p.split(', ')[0]))
    if len(lons_vecteur) > 2:
        centre = Polygon(np.column_stack((np.array(lons_vecteur), np.array(lats_vecteur)))).centroid
        centre_lat = centre.coords[0][1]
        centre_lon = centre.coords[0][0]
        insert_geoloc = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
        insert_geoloc.adresse_geolocalisation(centre_lat, centre_lon, result[0], 'REF_LEAFLET')

# pour tous ceux qui n'ont pas de ref_leaflet, mettre le point déjà indiqué en base: soit le point de geometry_geolocalisation, soit le point de geolocalisation si pas de geometry associée
for result in curs.execute("""with rangs as (select a.adresseid, a.geometryid , rank() over(partition by a.adresseid, a.indice, case when  c.geometrytype = 'relation' then 1
	when c.geometrytype = 'way' then 2 else 3 end ,case when  c.geometryareatype = 'BATIMENT' then 1
	when c.geometryareatype = 'VOIE' then 2 else 3 end    order by a.adresseid, a.geometryid, a.indice desc, case when  c.geometrytype = 'relation' then 1
	when c.geometrytype = 'way' then 2 else 3 end ,case when  c.geometryareatype = 'BATIMENT' then 1
	when c.geometryareatype = 'VOIE' then 2 else 3 end ) as rang
from adresse_geometry a
inner join geometry_geolocalisation b on b.geometryid = a.geometryid and b.polygone is  null
inner join geometry c on c.geometryid = a.geometryid 
group by a.adresseid, a.geometryid)
select a.adresseid, a.geometryid, b.latitude, b.longitude
from rangs a
inner join geometry_geolocalisation b on b.geometryid = a.geometryid
left join geolocalisation c on c.adresseid = a.adresseid and c.relationtype = 'REF_LEAFLET'
where a.rang = 1 and c.geolocalisationid is null""").fetchall():
    insert_geoloc = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
    insert_geoloc.adresse_geolocalisation(result[2], result[3], result[0], 'REF_LEAFLET')

for result in curs.execute("""select a.adresseid, c.latitude, c.longitude
from adresse a
left join geolocalisation b on b.adresseid = a.adresseid and b.relationtype = 'REF_LEAFLET'
inner join geolocalisation  c on c.adresseid = a.adresseid and c.relationtype = 'COORDONNEES_INVENTAIRE' and c.latitude is not null and c.longitude is not null
where b.geolocalisationid is null""").fetchall():
    insert_geoloc = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
    insert_geoloc.adresse_geolocalisation(result[1], result[2], result[0], 'REF_LEAFLET')
'''
conn.close()

