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
from centerline.geometry import Centerline
import pandas as pd
import csv
import numpy as np
import urllib.parse
from utils.insert_geometries import Insert
import unidecode
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

#init_specific_tables(conn, curs)

# RUE d'abord:
for r in curs.execute("select * from adresse left join geolocalisation on geolocalisation.adresseid = adresse.adresseid left join adresse_geometry on adresse_geometry.adresseid = adresse.adresseid where adresse_geometry.geometryid  is null  and adresse.rue not like '%,%PONT%' and adresse.rue is not null  and adresse.rue not like '48%' and adresse.numero is null order by adresse.rue limit 5").fetchall():
    print(r)
    # je cherche dans osm par le name  
    results = curs_osm.execute("select   \"addr:housenumber\",\"addr:street\", osmid , lat , lon , name, wikidata, osmtype from data where osmtype = 'relation' and (  \"addr:street\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', str(r[1]))).lower().replace(" ", "%").replace("'", "''").replace("’", "''") if r[1] else None)+"' or  \"name\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', str(r[1]))).lower().replace(" ", "%").replace("'", "''").replace("’", "''") if r[1] else None)+"' or  \"name\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', unidecode.unidecode(str(r[1]))).lower().replace(" ", "%").replace("'", "''").replace("’", "''")) if r[1] else None)+"'  or  \"addr:street\" like '"+((re.sub(r'^([^,]+), ?(.*)', r'\2%\1', unidecode.unidecode(str(r[1]))).lower().replace(" ", "%").replace("'", "''").replace("’", "''")) if r[1] else None)+"')" ).fetchall()
    if results:
        indice_confiance = (100 if len(results) == 1 else 100/len(results))
        polygone_relation = []
        for result in results:
            print(result)
            relationid = get_geometryid_from_osmid(result[2], result[7], 'VOIE', curs, URL_ROOT)
            print(relationid)
            ways = json.loads((requests.get(URL_ROOT + "/get_ways/"+str(result[7])+"/"+str(result[2]) )).text)
            print(ways)
            '''
            for way in ways["elements"][0]["members"]:
                # je récupère les ways de role street qui constituent la rue (relation)
                if way["type"] == "way" and "role" in way and way["role"] == "street":
                    wayid = get_geometryid_from_osmid(way["ref"], way["type"], 'VOIE', curs, URL_ROOT)
                    # créer les ways
                    #insert_object('way', 'VOIE',way["ref"],wayid, libellefre=result[5], area_matched=[relationid], indice_confiance=indice_confiance, wikidataid=result[6])
                    polygone_way = get_nodes('way', wayid, way["ref"], lons_vect, lats_vect, geometryid_vect, curs_osm, curs)
                    polygone_relation = polygone_relation + polygone_way
                    # centre du polygone pour area_matched
                    centre_lat, centre_lon = get_centre_polygon(polygone_way)
                    area_matched, area_around = is_in_geometry(centre_lat, centre_lon, lons_vect, lats_vect, geometryid_vect)
                    #insert_object('way', 'VOIE',way["ref"],wayid,  area_matched=area_matched)
            # calculer le polygone de la relation à partir des polygones ways
            centre_lat, centre_lon = get_centre_polygon(polygone_relation)
            area_matched, area_around = is_in_geometry(centre_lat, centre_lon, lons_vect, lats_vect, geometryid_vect)
            # la relation adresse_geometry n'espas insérer et très mal quand elle l'est
            #insert_object('relation', 'VOIE',result[2],relationid, libellefre=result[5], area_matched=area_matched, indice_confiance=indice_confiance, wikidataid=result[6], adresseid=result[0])
            '''
conn.close()