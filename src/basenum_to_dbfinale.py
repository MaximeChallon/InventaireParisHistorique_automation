import csv 
import os
import dotenv
import sys
import dateparser
import re
import json
import requests
from datetime import date
import dateutil
from utils.extract_keywords import keywords


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))

DEBUG = os.environ['DEBUG']
KEY_WS = os.environ["KEY_WS"]
URL_ROOT  = os.environ["URL_ROOT"]
IMG_DESTDIR = os.environ["IMG_DESTDIR"]
try:
    os.system("rm " + BASE_DIR + "/num_error.csv")
except:
    pass

csv_path = "/home/maxime/Téléchargements/PH/Requête6.csv"
img_sourcedir = "/home/maxime/Téléchargements/PH/2000/"

max = 5
min = 0

with open(csv_path, "r") as f:
    f_o = csv.reader(f, delimiter="|")
    next(f_o)
    l = 0
    i =0
    ok = 0
    ko = 0
    liste_num_traites = [] # nécessaire pour sauter les doublons de lignes du fichier d'entrée
    for line in f_o:
        if i <= max and l>= min and line[1] not in liste_num_traites:
            post_headers = {"ws-key": KEY_WS}
            post_data = {"type": "PHOTO"}
            id_metier = line[1]
            designation = line[2]
            date_prise_vue = line[10]
            annee_prise_vue  = line[11]
            commentaire = line[13]
            cote_base_num = line[15]
            date_entree_base = line[18]
            nom_fichier = line[19]
            nature_photo = line[28]
            architecte  = line[39]
            date_construction = line[40]
            commentaire2 = line[41]
            mh = line[47]
            photographe = line[52]
            droit = line[54]
            mots_cles = line[57]
            rue = line[62]
            n_rue = line[63] + line[64]
            code_postal = line[66]
            ville = line[67]
            latitude = line[76]
            longitude = line[77]
            couleur = line[79]
            support = line[81]
            generalite = line[84]
            arrondissement = re.sub( r'[^0-9]+', '', line[0])

            if id_metier:
                post_data["Id_metier"] = id_metier
                post_data["Id_metier_type"] = "Identifiant de la base de numérisations"
            if cote_base_num:
                post_data["Cote_base"] = cote_base_num
            if designation:
                designation = (re.sub(r"([^\_ \.]+\_)+[^\_ \.]+", "",designation)).strip().upper()
                if designation != "" and designation:
                    post_data["Site"] = designation
                else:
                    post_data["Site"] = "IMMEUBLE"
            if annee_prise_vue:
                post_data["Prise_vue"] =annee_prise_vue
            if date_prise_vue:
                try:
                    date_prise_vue = (dateparser.parse(date_prise_vue ).date()).strftime("%Y/%m/%d")
                except:
                    date_prise_vue = None
                if date_prise_vue:
                    post_data["Prise_vue"] =date_prise_vue
            if commentaire:
                post_data["Note"] =  commentaire
            if commentaire2:
                post_data["Note2"] =  commentaire2
            if date_entree_base:
                post_data["Entree_base_num"] = (dateparser.parse(date_entree_base, settings={'DEFAULT_LANGUAGES': ["fr"]})).strftime("%Y/%m/%d")
            if architecte:
                post_data["Architecte"] = architecte
            if date_construction:
                post_data["Construction"] = date_construction
            if ("classé" in mh or "classe" in mh or "Classé" in mh) and "non" not in mh and "Non" not in mh:
                post_data["MH"] = "OUI"
            else:
                post_data["MH"] = "NON"
            if photographe:
                post_data["Photographe"] = (re.sub(r'(.*) ([^ ]+)', r'\2, \1',photographe)).upper()
            if arrondissement:
                post_data["Arrondissement"] = arrondissement
            if rue:
                rue_norm = re.sub(r"(.*) (((COURS)|(RUE)|(PLACE)|(BOULEVARD)|(AVENUE)|(QUAI)|(COUR)) ((D((E(S)?)|(U)).*)|(L.*))?)$", r"\1, \2", rue.upper())
                post_data["Rue"] = rue_norm
            if n_rue:
                post_data["N_rue"] = n_rue
            if line[0]:
                post_data["Ville"] = re.sub(r' .*', '', line[0]).upper()
            if post_data["Ville"] == "PARIS":
                post_data["Departement"] = "75"
            if latitude:
                post_data["Latitude"] = latitude.replace(',', '.')
            if longitude:
                post_data["Longitude"] = longitude.replace(',', '.')
            #TODO: si y a pas les coordonnées gps, alors apperler l'api du gvt
            if couleur:
                post_data["Couleur"] = couleur.upper()
            if support:
                if support == "Numérique":
                    post_data["Support"]= "NUMERIQUE"
                else:
                    post_data["Support"] = support.upper()
            if generalite:
                if generalite.upper() == 'ARCHITECTURE PRIVÉE':
                    post_data["Generalite"] = "PRIVÉE"
                elif generalite.upper() == 'ARCHITECTURE PUBLIQUE':
                    post_data["Generalite"] = "PUBLIQUE"
                else :
                    post_data["Generalite"] =generalite.upper().replace('ARCHITECTURE ', '').replace(', ', '_').replace(' ET ', '_').replace('RAIRES', 'RAIRE')
            #extraction de mots-clés
            mots_cles_commentaire = keywords(commentaire).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")

            mots_cles_commentaire2 = keywords(commentaire2).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")

            mots_cles_motscles = keywords(mots_cles).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")

            mots_cles_designation = keywords(line[3] + "." + line[2]).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")
            motscles = list(dict.fromkeys(mots_cles_commentaire + mots_cles_commentaire2 + mots_cles_motscles + mots_cles_designation))
            if motscles:    
                post_data["Mots_cles"] = str(   motscles  )

            post_data["Cote"] = "BASE_NUM"
            # activité d'inventaire
            post_data["Auteur"] = "OUTIL DE MIGRATION AUTOMATIQUE"
            post_data["Date_inventaire"] = (date.today()).strftime("%Y/%m/%d")
            # générer un num inventaire avant envoi si l'instance n'existe pas encore
            try:
                num_inv = json.loads(requests.post(URL_ROOT + "/select/" + str(id_metier) ,data=json.dumps({"type":"Identifiant de la base de numérisations"}),  headers={"ws_key":KEY_WS}).content)["num_inv"]
            except:
                num_inv = (json.loads(requests.post(URL_ROOT + "/create_inventory_number/" + "BASE_NUM").content))["Numero_inventaire"]
            r = requests.post(URL_ROOT + "/insert/" + str(num_inv), data=json.dumps(post_data), headers=post_headers)
            if r.status_code > 400:
                ko += 1
                # mettre le num d'inv en mémoire quelque part avec son message
                with open(BASE_DIR + "/num_error.csv", "a") as f:
                    f_o = csv.writer(f)
                    f_o.writerow([line[0]])
            else:
                ok +=1
            i+=1
            liste_num_traites.append(line[1])
            sys.stdout.write("\r" + "Process " + str(line[1]) + " -- OK : "+str(ok) +" -- KO : "+str(ko))
            sys.stdout.flush()
        l += 1
    print("\n")