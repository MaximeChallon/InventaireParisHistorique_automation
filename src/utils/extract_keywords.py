import nltk
from rake_nltk import Rake
import re
import json
from sqlalchemy import create_engine
import sqlite3
from googletrans import Translator, constants
import unidecode

OSM_TAGS_KEPT = [  "addr:city" ,  "addr:housename",  "addr:housenumber",    "addr:place",  "addr:postcode",  "addr:province",   "addr:street",  "addr:streetnumber",   "building",   "amenity",  "bridge",  "historic",  "int_name",  "int_ref",  "landuse",  "leisure",  "loc_name",  "loc_ref",  "man_made",  "military",  "name",  "nat_name",  "nat_ref",  "natural",  "office",  "official_name",  "operator",  "place",  "postal_code",  "ref",  "shop",  "short_name",  "tourism",  "waterway",  "wikipedia", "wikidata"]

def translate(chaine, from_, to_):
    translator = Translator()
    translation = translator.translate(str(chaine),src=from_, dest=to_)
    return unidecode.unidecode(re.sub(r'^((u|U)ne? )(.*)$', r'\3',str(translation.text)).upper().replace(" ", "_"))

def tags_to_descriptive_data(tags):
    data = {"mots_cles":[]}
    for k in tags.keys():
        if k in OSM_TAGS_KEPT and tags[k]:
            if k.startswith("addr"):
                data["num_rue"] = tags["addr:streetnumber"] if "addr:streetnumber" in tags else None
                data["rue"] = tags["addr:street"] if "addr:street" in tags else None
                data["ville"] = tags["addr:city"] if "addr:city" in tags else None
                data["code_postal"] = tags["addr:postcode"] if "addr:postcode" in tags else None
            elif k == "name":
                data["nom_site"] = tags["name"] 
            elif k == "wikidata" or k=="wikipedia":
                data[k] = tags[k]
            elif tags[k] == "yes" or tags[k] == "no":
                # alors k est un mot clé concept posé sur la gemoetry que si yes
                if tags[k] == "yes":
                    data["mots_cles"].append(translate(k.replace("_", " "),"en", "fr").upper())
            elif k in [  "building",   "amenity",  "bridge",  "man_made",  "shop", "historic"]:
                #alors la valeur de tags[k] est le mot clé concept
                data["mots_cles"].append(translate("a " +tags[k].replace("_", " "),"en", "fr").upper())
            else:
                # TODO: pour le reste, ça devient des textes
                pass
    return data

class Connect():
    def __init__(self, connexion_infos):
        self.connexion_infos = connexion_infos

    def sqlite(self):
        engine = create_engine(self.connexion_infos["db_system"] + ':///' + self.connexion_infos["db_url"], echo = False)
        return engine

class keywords():
    def __init__(self, texte):
        nltk.download('stopwords', quiet=True)
        nltk.download('punkt', quiet=True)
        self.texte = re.sub(r'[^\w\s]', ' .',texte.replace(" ", "."))

    def extract(self):
        r =Rake(language='french')
        r.extract_keywords_from_text(self.texte)
        keywords_extracted = r.get_ranked_phrases()
        # p sert à mettre au singulier
        p = nltk.PorterStemmer()
        return [(p.stem(keyword)).upper().replace(" ", "_") for keyword in keywords_extracted]

    def join_with_sql_referentiel(self, connexion_infos, referentielid):
        db = Connect(connexion_infos).sqlite()
        results =  db.execute("select * from concept where referentielid ='"+referentielid+"' and code in "+"('" + "','".join(self.extract()) + "')"+"").fetchall()
        return [r[1] for r in results]
    
    def from_base_osm(self,base_osm,osmid):
        conn_osm = sqlite3.connect(base_osm)  
        curs_osm = conn_osm.cursor()
        results = curs_osm.execute("select 		\"building\"	,	\"amenity\"	,	\"bridge\"	,	\"historic\"	,	\"int_name\"	,	\"int_ref\"	,	\"landuse\"	,	\"leisure\"	,	\"loc_name\"	,	\"loc_ref\"	,	\"man_made\"	,	\"military\"	,	\"name\"	,	\"nat_name\"	,	\"nat_ref\"	,	\"natural\"	,	\"office\"	,	\"official_name\"	,	\"operator\"	,	\"place\"	,	\"postal_code\"	,	\"ref\"	,	\"shop\"	,	\"short_name\"	,	\"tourism\"	,	\"waterway\"	,	\"wikipedia\"	,	\"wikidata\"	 from data where osmid = '"+str(osmid)+"'")
        r = [dict((curs_osm.description[i][0], value) \
               for i, value in enumerate(row)) for row in results.fetchall()]
        try:
            tags = tags_to_descriptive_data(r[0])
        except:
            tags=[]
        conn_osm.close()
        return tags

