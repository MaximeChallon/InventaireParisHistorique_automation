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
from utils.mdb_tools import Mdb
import sqlite3


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

# ubuntu: accès au serveur: /run/user/1000/gvfs/smb-share:server=freebox-server.local,share=disque%20dur/phototheque
csv_path = "/home/maxime/Téléchargements/PH/Requête6.csv"
img_sourcedir = "/home/maxime/Téléchargements/PH/"
img_outdir = "/home/maxime/Téléchargements/PH/out/"
mdb_filepath = "/home/maxime/Téléchargements/Phototheque.mdb"
sql_filepath = "/home/maxime/Téléchargements/Phototheque.sqlite"
csv_filepath = "/home/maxime/Téléchargements/Phototheque.csv"
working_dir = BASE_DIR + "/out/"
try:
    os.system("rm -r " + working_dir)
except:
    pass

try:
    os.system("mkdir " + working_dir)
except:
    pass

entree_mdb = Mdb(mdb_filepath)
# migrer mdb vers sqlite: prend 20 minutes
#entree_mdb._to_sqlite(sql_filepath)

# créer le csv à partir requete sql: boucler sur les sites afin de faire un csv par arrondissement
# TODO: remove limit and order for complete run and where
differents_sites = """
    select a.Id, a.cle , count(*)
    from Series a
    inner join Sites c on c.IdSerie = a.Id
    inner join Photos b on b.IdSite = c.Id
    where a.cle = '20000'
    group by a.cle
    order by count(*);
"""
conn = sqlite3.connect(sql_filepath)  
curs = conn.cursor()
sites  = curs.execute(differents_sites).fetchall()
for site in sites:
    print("Traitement du site " +str(site[1]) + " , " + str(site[2]) + " entrées")
    requete = """
        SELECT Series."Designation",a."Id",a."Designation",a."IdSite","IdSousLocalisation","IdEvenementGenerateur","IdPhotographe","IdDroits","IdSupport","IdCouleur","DatePriseDeVue",
        "AnneePriseDeVue","CaracteristiquesOriginal",a."Commentaires",a."Commentaires_rtf","Cote","Documentee","ASupprimer","DateEntreeBase","NomFichier",a."ACompleter",
        a."Origine",a."FM_Serie","FM_Droits","FM_Support","FM_Photographe",a."FM_MotsCles","FM_Couleur","NaturePhoto",a."Solde","FM_Id","APublier","NumeroContrecolle",
        "StatutContrecolle","AVendre",a."Supprimer",b."Id",b."Designation","IdTypeClassement","Architectes","SiecleConstruction",b."Commentaires",b."Commentaires_rtf",
        b."ACompleter",b."Origine","DateCreation",b."FM_Serie","FM_TypeClassement",b."FM_MotsCles",b."Solde",b."Supprimer",Photographes."Id",Photographes."Designation",
        Droits."Id",Droits."Designation",PhotosMotsCles."Id","IdPhoto","IdpMotCle",PhotosMotsCles."FM_MotCle",PhotosMotsCles."Solde",Adresses."Id",Adresses."IdSite",
        Voies."Designation","Numero","SuffixeNumero","Principale","CodePostal","Ville",Voies."FM_Voie",Adresses."Solde",Adresses."Supprimer","NumeroDebut","NumeroFin","Erreur",
        "libErreur","Paire","Latitude","Longitude",Couleurs."Id",Couleurs."Designation",Supports."Id",Supports."Designation",SitesMotsCles."Id",SitesMotsCles."IdSite",
        "IdsMotCle",SitesMotsCles."FM_MotCle",SitesMotsCles."Solde",MotsCles."Id",MotsCles."Designation","Niveau","Synonyme"
        FROM ((((((((photos AS a INNER JOIN sites AS b ON a.idSite = b.id) 
        LEFT JOIN Photographes ON a.IdPhotographe = Photographes.Id) 
        LEFT JOIN Droits ON a.IdDroits = Droits.Id) LEFT JOIN PhotosMotsCles ON a.Id = PhotosMotsCles.IdPhoto) LEFT JOIN Adresses ON b.Id = Adresses.IdSite)
        LEFT JOIN Couleurs ON a.IdCouleur = Couleurs.Id) LEFT JOIN Supports ON a.IdSupport = Supports.Id) left JOIN SitesMotsCles ON b.Id = SitesMotsCles.IdSite) 
        left JOIN MotsCles ON SitesMotsCles.IdsMotCle = MotsCles.Id
        left join Voies ON Voies.Id = Adresses.IdVoie
        left join Series ON Series.Id = b.IdSerie
                 where Principale = 1 and (((b.idSerie)="""+str(site[0])+"""))   ; 
    """
    entree_mdb._request_to_csv(requete, working_dir + str(site[0]) + ".csv", sql_filepath)

    max = 20
    min = 0

    with open(working_dir + str(site[0]) + ".csv", "r") as f:
        f_o = csv.reader(f, delimiter="|")
        next(f_o)
        l = 0
        i =0
        ok = 0
        ko = 0
        liste_num_traites = [] # nécessaire pour sauter les doublons de lignes du fichier d'entrée (créés à cause de multiples adresses ou mots-clés)
        liste_num_traites_succes = [] 
        for line in f_o:
            # filtrer sur seulement les lignes qui ont un fichier numérique de la photo qui existe, dont l'id n'a pas déjà été traité et dont i et l correspondent aux paramètres
            if i <= max and l>= min and line[1] not in liste_num_traites and line[15] and (os.path.exists(img_sourcedir + str(site[1]) + "/p_" + line[15] + ".jpg") or os.path.exists(img_sourcedir + str(site[1]) + "/p_" + line[15] + ".jpeg") or os.path.exists(img_sourcedir + str(site[1]) + "/p_" + line[15] + ".png")):
                sys.stdout.write("\r" + "Working " + str(line[1]) )
                sys.stdout.flush()
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
                    post_data["Ville"] = re.sub(r' [0-9]+.*', '', line[0]).upper()
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
                    elif support == "Papier" or support == "PAPIER":
                        post_data["Support"] = "TIRAGE PAPIER"
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
                #TODO: db paths en environement
                mots_cles_commentaire = keywords(commentaire).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")

                mots_cles_commentaire2 = keywords(commentaire2).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")

                mots_cles_motscles = keywords(mots_cles).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")
                
                mots_cles_designation = keywords(line[3] + "." + line[2]).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")
                try:
                    mots_base = curs.execute("""select group_concat(b.Designation)
                    from PhotosMotsCles a
                    left join MotsClesPhoto b on b.Id = a.IdpMotCle
                    where a.IdPhoto = """+str(id_metier)+"""
                    group by a.IdPhoto""").fetchall()[0][0]
                    mots_cles_base = keywords(mots_base ).join_with_sql_referentiel({'db_system': 'sqlite','db_url': '/home/maxime/dev/InventaireParisHistorique_services/app/db_finale.sqlite'},"049c90d062b5")
                except:
                    mots_cles_base = []
                motscles = list(dict.fromkeys(mots_cles_commentaire + mots_cles_commentaire2 + mots_cles_motscles + mots_cles_designation + mots_cles_base))
                if motscles:    
                    post_data["Mots_cles"] = str(   motscles  )

                post_data["Cote"] = "BASE_NUM"
                # activité d'inventaire
                post_data["Auteur"] = "OUTIL DE MIGRATION AUTOMATIQUE"
                post_data["Date_inventaire"] = (date.today()).strftime("%Y/%m/%d")
                # générer un num inventaire avant envoi si l'instance n'existe pas encore
                # print(post_data)
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
                    # si succès: copier l'image dans le fichier de destination si elle n'y est pas déjà
                    if not os.path.exists(img_outdir  + str(site[1]) + "/" + line[19]):
                        if not os.path.exists(img_outdir  + str(site[1]) + "/"):
                            os.system("mkdir " + img_outdir  + str(site[1]) + "/")
                        os.system("cp " + img_sourcedir  + str(site[1]) + "/" + line[19] + " " + img_outdir  + str(site[1]) + "/" + str(num_inv) + ".jpg")
                    ok +=1
                    liste_num_traites_succes.append(num_inv)
                i+=1
                liste_num_traites.append(line[1])
                sys.stdout.write("\r" + "Process " + str(line[1]) + " -- OK : "+str(ok) +" -- KO : "+str(ko))
                sys.stdout.flush()
            l += 1
        print("\n")
        #print("Numéros d'inventaire traités:"+ str(liste_num_traites_succes))

conn.close()