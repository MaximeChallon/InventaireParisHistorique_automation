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
where b.geolocalisationid is null and a.adresseid not like '<!%'""").fetchall():
    insert_geoloc = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
    insert_geoloc.adresse_geolocalisation(result[1], result[2], result[0], 'REF_LEAFLET')

conn.close()