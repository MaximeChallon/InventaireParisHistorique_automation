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

if not (os.path.exists(BASE_ADRESSES)):
    open(BASE_ADRESSES, "w").close()    
conn = sqlite3.connect(BASE_ADRESSES)  
curs = conn.cursor()

# je charge d'abord les objets quartiers, arrdsmt et paris et leur lats_vect et lons_vect
conn_osm = sqlite3.connect(BASE_OSM)  
curs_osm = conn_osm.cursor()

lats_vect, lons_vect, geometryid_vect = init_vects(curs)

init_specific_tables(conn, curs)

# PONTS d'abord:
for r in curs.execute("select * from adresse left join geolocalisation on geolocalisation.adresseid = adresse.adresseid left join adresse_geometry on adresse_geometry.adresseid = adresse.adresseid where adresse_geometry.geometryid  is null  and adresse.rue like '%,%PONT%' and adresse.rue is not null  and adresse.rue not like '48%' order by adresse.rue ").fetchall():
    print(r)
    # je cherche dans osm par le name and man_made bridge: tous les ponts ont un match 
    results = curs_osm.execute("select   \"addr:housenumber\",\"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where man_made = 'bridge' and  \"name\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1%', str(r[1]))).lower().replace(" ", "%").replace("'", "''").replace("’", "''") if r[1] else None)+"' " ).fetchall()
    if results:
        indice_confiance = (100 if len(results) == 1 else 100/len(results))
        for result in results:
            geometryid = get_geometryid_from_osmid(result[2], result[7], 'VOIE', curs, URL_ROOT)
            if result[7] == "node":
                # alors je peux chercher les geom pères car j'ai les gps
                area_matched, area_around = is_in_geometry(result[3], result[4], lons_vect, lats_vect, geometryid_vect)
                # insertion du result en base
                insert_object('node', 'VOIE',result[2],geometryid, libellefre=result[5], area_matched=area_matched, adresseid=r[0],indice_confiance=indice_confiance)
            else:
                # chercher les nodes du way ou de la relation pour avoir les coordonnées
                polygone_object = get_nodes(result[7], geometryid,result[2], lons_vect, lats_vect, geometryid_vect, curs_osm, curs)
                # obtenir le lat et lon du centre du polygon créé
                centre_lat, centre_lon = get_centre_polygon(polygone_object)
                area_matched, area_around = is_in_geometry(centre_lat, centre_lon, lons_vect, lats_vect, geometryid_vect)
                insert_object(result[7], 'VOIE',result[2],geometryid, libellefre=result[5], area_matched=area_matched, adresseid=r[0],indice_confiance=indice_confiance)

conn.close()