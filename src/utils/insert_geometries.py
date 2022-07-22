import sqlite3
import requests
import urllib
import json

from datetime import datetime

class Insert():
    def __init__(self, base_adresses, base_osm, url_root):
        self.BASE_ADRESSES = base_adresses 
        self.BASE_OSM = base_osm 
        self.URL_ROOT= url_root
        self.conn = sqlite3.connect(self.BASE_ADRESSES)
        self.curs = self.conn.cursor()
    
    def check_osmid_exists(self,osmid):
        exists = False
        if len(self.curs.execute("select * from geometry_identifier where identifiervalue = '"+osmid+"'").fetchall()) > 0:
            exists=True
        return exists
    
    def get_osmid_lat_lon(self, osmid):
        return self.curs.execute("select a.geometryid, b.latitude, b.longitude from geometry_identifier a inner join geometry_geolocalisation b on b.geometryid = a.geometryid  where a.identifiervalue = '"+osmid+"' and a.relationtype = 'OSMID'").fetchall()
    
    def geometry(self, type, areatype, osmid, geometryid):
        if len(self.curs.execute("select * from geometry where geometryid = '"+str(geometryid)+"' ").fetchall()) == 0:
            self.curs.execute("insert into geometry values ('"+geometryid+"', '"+type+"', '"+areatype+"')")
            self.conn.commit()
    
    def geometry_identifier(self, value, type, relationtype, geometryid):
        if len(self.curs.execute("select * from geometry_identifier where identifiervalue = '"+str(value)+"' and identifiertype = '"+type+"' and relationtype = '"+relationtype+"'  and geometryid = '"+str(geometryid)+"'").fetchall()) == 0 and value:
            self.curs.execute("insert into geometry_identifier (geometryid, identifierid, identifiervalue, identifiertype, relationtype) values ('"+geometryid+"', '"+(requests.get(self.URL_ROOT + "/get_identifiant/identifier/osm" + urllib.parse.quote(geometryid + type + relationtype + str(value )) )).text+"', '"+str(value)+"','"+str(type)+"','"+relationtype+"')")
            self.conn.commit()
    
    def geometry_geometry(self, source, target, relationtype):
        if len(self.curs.execute("select * from geometry_geometry where sourcegeometryid = '"+str(source)+"' and targetgeometryid = '"+target+"' and relationtype = '"+relationtype+"'").fetchall()) == 0 :
            self.curs.execute("insert into geometry_geometry (sourcegeometryid, targetgeometryid, relationtype) values ('"+source+"', '"+target+"', '"+relationtype+"')")
            self.conn.commit()

    def adresse_geometry(self, adresseid, geometryid, indice:int):
        if len(self.curs.execute("select * from adresse_geometry where adresseid = '"+str(adresseid)+"' and geometryid = '"+geometryid+"' ").fetchall()) == 0 :
            self.curs.execute("insert into adresse_geometry (adresseid,geometryid,indice) values ('"+adresseid+"', '"+geometryid+"', "+str(indice)+")")
            self.conn.commit()

    def geometry_concept(self, geometryid, keywords, base_concepts):
        if keywords:
            conn_concepts = sqlite3.connect(base_concepts)
            curs_concepts = conn_concepts.cursor()
            for mot in keywords["mots_cles"]:
                concept = curs_concepts.execute("select a.conceptid from concept_text a inner join concept b on b.conceptid = a.conceptid inner join referentiel c on c.referentielid = b.referentielid and c.code = 'MOT_CLE' where a.textvalue = '"+str(mot).replace("'", "''")+"' and a.texttype = 'Label' and a.textlang = 'fre'").fetchall()
                if len(concept) >0 :
                    conceptid = concept[0][0]
                else:
                    # créer le concept
                    conceptid = (requests.get(self.URL_ROOT + "/get_identifiant/concept/osm2" + urllib.parse.quote(geometryid + str(mot).replace("'", "''") ) )).text
                    curs_concepts.execute("insert into concept (conceptid, code, referentielid) values ('"+conceptid+"', '"+str(mot).replace("'", "''")+"', '"+curs_concepts.execute("select c.referentielid from referentiel c where c.code = 'MOT_CLE' ").fetchall()[0][0]+"')")
                    conn_concepts.commit()
                    conn_concepts.execute("insert into concept_text (conceptid, textid, textvalue, textlang,  texttype) values ('"+str(conceptid)+"', '"+(requests.get(self.URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + str(mot).replace("'", "''") ) )).text+"', '"+str(mot).replace("'", "''")+"', 'fre',  'Label')")
                    conn_concepts.commit()
                if len(self.curs.execute("select * from geometry_concept where conceptid = '"+str(conceptid)+"' and geometryid = '"+geometryid+"' and relationtype = 'Mot clé'").fetchall()) == 0 :
                    self.curs.execute("insert into geometry_concept (geometryid, conceptid, relationtype) values ('"+geometryid+"', '"+conceptid+"', 'Mot clé')")
                    self.conn.commit()
            
            conn_concepts.close()
        
    def geometry_text(self, value, lang, relationtype, geometryid):
        if len(self.curs.execute("select * from geometry_text where textvalue = '"+str(value).replace("'", "''")+"' and textlang = '"+lang+"' and relationtype = '"+relationtype+"' and geometryid = '"+str(geometryid)+"'").fetchall()) == 0 and value:
            try:
                self.curs.execute("insert into geometry_text (geometryid, textid, textvalue, textlang, relationtype) values ('"+geometryid+"', '"+(requests.get(self.URL_ROOT + "/get_identifiant/text/osm" + urllib.parse.quote(geometryid + lang + relationtype + str(value ).replace("'", "''")) )).text+"', '"+str(value).replace("'", "''")+"','"+str(lang)+"','"+relationtype+"')")
                self.conn.commit()
            except:
                pass

    def geometry_geolocalisation(self, lat, lon, geometryid, polygone):
        if len(self.curs.execute("select * from geometry a inner join geometry_geolocalisation b on b.geometryid = a.geometryid and a.geometryid = '"+geometryid+"'").fetchall()) == 0 and (lat or polygone):
            if not polygone:
                self.curs.execute("insert into geometry_geolocalisation (geometryid, geolocalisationid, latitude, longitude) values ('"+geometryid+"', '"+(requests.get(self.URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(geometryid + str(lat) + str(lon) ) )).text+"', "+str(lat)+","+str(lon)+")")
            else:
                self.curs.execute("insert into geometry_geolocalisation (geometryid, geolocalisationid, polygone) values ('"+geometryid+"', '"+(requests.get(self.URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(geometryid + str(polygone) ) )).text+"', '"+str(polygone)+"')")
            self.conn.commit()

    def adresse_geolocalisation(self, lat, lon, adresseid, relationtype):
        if len(self.curs.execute("select * from geolocalisation a where a.adresseid = '"+adresseid+"' and a.relationtype = '"+str(relationtype)+"'").fetchall()) == 0 and str(lat).startswith('4'):
            try:
                self.curs.execute("insert into geolocalisation (adresseid, geolocalisationid, latitude, longitude, relationtype) values ('"+adresseid+"', '"+(requests.get(self.URL_ROOT + "/get_identifiant/geolocalisation/osm" + urllib.parse.quote(adresseid + str(lat) + str(lon) + str(relationtype)) )).text+"', "+str(lat)+","+str(lon)+", '"+str(relationtype)+"')")
                self.conn.commit()
            except Exception as e:
                pass

    def update_geometry_geolocalisation(self, lat, lon, geolocalisationid, geometryid, polygone):
        if len(self.curs.execute("select * from geometry a inner join geometry_geolocalisation b on b.geometryid = a.geometryid and a.geometryid = '"+geometryid+"'").fetchall()) > 0 and (lat or polygone):
            if not polygone:
                self.curs.execute("UPDATE geometry_geolocalisation SET lat = "+str(lat)+", lon = "+str(lon)+"  WHERE geolocalisationid = '"+str(geolocalisationid)+"'")
            else:
                self.curs.execute("UPDATE geometry_geolocalisation SET polygone = '"+str(polygone)+"'  WHERE geolocalisationid = '"+str(geolocalisationid)+"'")
            self.conn.commit()

    def __exit__(self):
        self.conn.close()