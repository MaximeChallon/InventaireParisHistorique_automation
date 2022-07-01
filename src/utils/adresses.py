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

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), os.pardir)
dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))


DEBUG = os.environ['DEBUG']
KEY_WS = os.environ["KEY_WS"]
URL_ROOT  = os.environ["URL_ROOT"]
CSV_INVENTAIRE = os.environ["CSV_INVENTAIRE"]
BASE_ADRESSES = BASE_DIR + "/out/base_adresses.sqlite"
BASE_OSM = BASE_DIR + "/out/osm.sqlite"
BASE_INVENTAIRE = "/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite"

def init_specific_tables(conn, curs):
    curs.execute("create table if not exists adresse_geometry (adresseid varchar(36) not null, geometryid varchar(36) not null, indice integer);")
    conn.commit()

    curs.execute("create table if not exists geometry_concept (geometryid varchar(36) not null, conceptid varchar(36) not null, relationtype varchar(36) null);")
    conn.commit()

def init_vects(curs):
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
    return lats_vect, lons_vect, geometryid_vect

def get_nodes(type_geom, geometryid, osmid, lons_vect, lats_vect, geometryid_vect, curs_osm, curs):
    noeuds = json.loads((requests.get(URL_ROOT + "/get_nodes/"+str(type_geom)+"/"+str(osmid) )).text)
    polygone_o = []
    insert_node_osm = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
    for noeud in noeuds["elements"]:
        type_noeud = noeud["type"]
        id_noeud = noeud["id"]
        if "nodes" in noeud:
            noeuds_osm = noeud["nodes"]
            contenu_dans = []
            for n_osm in noeuds_osm:
                if not insert_node_osm.check_osmid_exists(str(n_osm)):
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
                                pass
                            if rr_osm[5]:
                                insert_node_osm.geometry_text(str(rr_osm[5]), "fre", "LIBELLE", noid)
                                pass
                            insert_node_osm.geometry_geometry(noid, geometryid, 'COMPRIS DANS')
                            insert_node_osm.geometry_concept(noid, keywords("").from_base_osm(BASE_OSM, rr_osm[2]), BASE_INVENTAIRE)
                            insert_node_osm.geometry_geolocalisation(geometryid=noid, lat=rr_osm[3], lon=rr_osm[4], polygone=None)
                            area_matched2, area_around2 = is_in_geometry(rr_osm[3], rr_osm[4], lons_vect, lats_vect, geometryid_vect)
                            for a in area_matched2:
                                if a not in contenu_dans:
                                    contenu_dans.append(a)
                else:
                    res = insert_node_osm.get_osmid_lat_lon(str(n_osm))
                    if res:
                        polygone_o.append([float(res[0][1]), float(res[0][2])])
                        area_matched2, area_around2 = is_in_geometry(res[0][1], res[0][2], lons_vect, lats_vect, geometryid_vect)
                        for a in area_matched2:
                            if a not in contenu_dans:
                                contenu_dans.append(a)
                    
            for area2 in contenu_dans:
                insert_node_osm.geometry_geometry(geometryid, area2, 'COMPRIS DANS')   
                pass     
    insert_node_osm.geometry_geolocalisation(geometryid=geometryid, lat=None, lon=None, polygone=polygone_o) 
    return polygone_o

def get_centre_polygon(polygone):
    lats_vecteur = []
    lons_vecteur = []
    centre_lat = None
    centre_lon = None
    for p in polygone:
        lons_vecteur.append(float(p[1]))
        lats_vecteur.append(float(p[0]))
    if len(lons_vecteur) > 2:
        centre = Polygon(np.column_stack((np.array(lons_vecteur), np.array(lats_vecteur)))).centroid
        centre_lat = centre.coords[0][1]
        centre_lon = centre.coords[0][0]
    return centre_lat, centre_lon

def get_shapely_polygon(polygone):
    lats_vecteur = []
    lons_vecteur = []
    polygon= None
    for p in polygone:
        lons_vecteur.append(float(p[1]))
        lats_vecteur.append(float(p[0]))
    if len(lons_vecteur) > 2:
        polygon = Polygon(np.column_stack((np.array(lons_vecteur), np.array(lats_vecteur))))
    return polygon


def insert_object(geometrytype, geometryareatype, geometryosmid, geometryid, wikidataid=None, libellefre=None, area_matched=None, adresseid=None, indice_confiance=None):
    insert = Insert(BASE_ADRESSES, BASE_OSM, URL_ROOT)
    insert.geometry(geometrytype, geometryareatype, geometryosmid, geometryid)
    insert.geometry_identifier(str(geometryosmid), 'IDEXT', 'OSMID', geometryid)
    if wikidataid:
        insert.geometry_identifier(wikidataid, 'IDEXT', 'WIKIDATA', geometryid)
    if libellefre:
        insert.geometry_text(str(libellefre), "fre", "LIBELLE", geometryid)
    if area_matched:
        for area in area_matched:
            insert.geometry_geometry(geometryid, area, 'COMPRIS DANS')
    if adresseid:
        insert.adresse_geometry(adresseid, geometryid, indice_confiance)
    insert.geometry_concept(geometryid, keywords("").from_base_osm(BASE_OSM, geometryosmid), BASE_INVENTAIRE)
    

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