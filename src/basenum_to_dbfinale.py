import csv 
import os
import dotenv
import sys
import dateparser
import re
import json
import requests
import pandas as pd
import numpy as np
from datetime import date
import dateutil
from utils.extract_keywords import keywords
from utils.mdb_tools import Mdb
import sqlite3
import datetime

def log(message):
    print("[" + str(datetime.datetime.now()) + "] " +  message)

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
img_sourcedir = "/home/maxime/Téléchargements/PH_base_num/Images/"
img_outdir = BASE_DIR + "/out/Images/"
IMG_DESTDIR = img_outdir
mdb_filepath = "/home/maxime/Téléchargements/Phototheque.mdb"
sql_filepath = "/home/maxime/Téléchargements/Phototheque.sqlite"
csv_filepath = "/home/maxime/Téléchargements/Phototheque.csv"
working_dir = BASE_DIR + "/out/"
ninv_debut_serie = 11684
liste_mots_cles = ["AFFICHAGE","ARCADE","BALCON","CANAL","CARIATIDE","CELLIER","CHANTIER","CHAPITEAU","CHARPENTE","CHEMINÉE","CLOCHER","COLOMBAGE","COLONNE","COMMERCE","COUPOLE","COUR","DÉCORATION","DÉMOLITION","ÉDIFICE_RELIGIEUX","ENSEIGNE","ESCALIER","ÉVÉNEMENT","EXTÉRIEUR","FAÇADE","FENÊTRE","FERRONNERIE","FESTIVAL_DU_MARAIS","FONTAINE","FOUILLES_ARCHÉOLOGIQUES","FRONTON","GOTHIQUE","HALLE","HORLOGE","INSCRIPTION","INTÉRIEUR","JARDIN","KIOSQUE","LUCARNE","MAISON","MAQUETTE","MASCARON","MOBILIER","MOULURE","MUR","NICHE","OCULUS","ORGUE","PASSAGE","PEINTURES_MURALES","PLAFOND","PLAN","PLAQUE","PONT","PORTE","POUTRES","PUITS","SCULPTURE","SEINE","STATUE","TOIT","TOUR","VERRIÈRE","VIE_ASSOCIATION","VITRAUX","VOÛTE","VUE_AÉRIENNE","VUE_ENSEMBLE"]

try:
    os.system("rm -r " + working_dir)
except:
    pass

try:
    os.system("mkdir " + working_dir)
except:
    pass

#entree_mdb = Mdb(mdb_filepath)
#migrer mdb vers sqlite: prend 20 minutes
#entree_mdb._to_sqlite(sql_filepath)

#1 - sortir un csv depuis sqlite
requete = """ with generalites as (select d.Id, group_concat(j.Designation, '/') as Designation 
    from Sites d
    left join SitesMotsCles l on l.IdSite = d.Id left join MotsClesSite j on j.Id = l.IdsMotCle
    group by d.Id),
    mots_cles as (select a.Id, c.Designation, rank() over( partition by a.Id order by  c.Designation) as rang
    from Photos a
    left join PhotosMotsCles b on b.IdPhoto = a.Id
    left join MotsClesPhoto c on c.Id = b.IdpMotCle),
    adresses_secondaires_tmp as (select a.Id,(case when s.Numero = '0' then '' else s.Numero end ) || ' ' ||  t.Designation as designation , rank() over(partition by a.Id order by t.Designation, s.Numero) as rang
    from Photos a
    inner join  Adresses s on s.IdSite = a.IdSite and s.Principale = 0  left join Voies t on t.Id = s.IdVoie),
    adresses_secondaires as (select t.Id , group_concat(t.designation, ' -- ') as designation
    from adresses_secondaires_tmp t
    group by t.Id),
    notes as (select a.Id, (case when d.Commentaires is not null  then '[Commentaire site] ' ||  replace(GROUP_CONCAT(DISTINCT d.Commentaires), ',', ' -- ')|| char(10) else '' end) as commentaire_site,
        (case when y.Designation is not null then '[Sous-localisations] ' ||  replace(GROUP_CONCAT(DISTINCT y.Designation), ',', ' -- ')|| char(10) else '' end ) as sousloc,
        (case when x.Designation is not null then '[Quartiers] ' ||  replace(GROUP_CONCAT(DISTINCT x.Designation), ',', ' -- ')|| char(10) else '' end ) as quartier
    from Photos a
    left join Sites d on d.Id = a.IdSite
    left join SousLocalisations y on y.IdSite = a.IdSite and y.Designation != 'Sans objet'
    left join SitesQuartiers w on w.IdSite = a.IdSite left join Quartiers x on x.Id = w.IdQuartier
    group by a.Id)
    select null as n_inventaire, c.Designation as rue, (case when b.Numero = '0' then null else b.Numero end )  || b.SuffixeNumero as n_rue, d.Designation as site, 
        e.Designation as arrdt, (case when b.Ville is null then 'PARIS' else b.Ville end ) as ville, (case when b.CodePostal is null then '75' else substr(b.CodePostal,0,2) end ) as dpt, 
        b.Latitude as lat, b.Longitude  as lon, g.Designation as support, f.Designation as couleur, null as taille, a.DatePriseDeVue as date_prise_vue,
         i.Designation as photographe,  h.Designation as droits, null as don ,  null as collection,
        d.SiecleConstruction as date_construction, d.Architectes as  architecte, k.Designation as mh,
        a.Commentaires as legende, l.Designation as generalite, m.Designation as m1, n.Designation as m2, o.Designation as m3, p.Designation as m4, q.Designation as m5, r.Designation as m6,
        s.designation as autre_adresse, t.commentaire_site || t.quartier ||t.sousloc as notes, a.Cote as cote_base, null as cote_physique,  a.DateEntreeBase as date_inventaire, upper(i.Designation) as auteur
    from Photos a
    left join Adresses b on b.IdSite = a.IdSite left join Voies c on c.Id = b.IdVoie
    left join Sites d on d.Id = a.IdSite left join Series e on e.Id = d.IdSerie left join TypesClassement k on k.Id = d.IdTypeClassement
    left join Couleurs f on f.Id = a.IdCouleur left join Supports g on g.Id = a.IdSupport
    left join Droits h on h.Id = a.IdDroits left join Photographes i on i.Id = a.IdPhotographe
    left join generalites l on l.Id = d.Id
    left join mots_cles m on m.Id = a.Id and m.rang = 1
    left join mots_cles n on n.Id = a.Id and n.rang = 2
    left join mots_cles o on o.Id = a.Id and o.rang = 3
    left join mots_cles p on p.Id = a.Id and p.rang = 4 
    left join mots_cles q on q.Id = a.Id and q.rang = 5
    left join mots_cles r on r.Id = a.Id and r.rang = 6
    left join  adresses_secondaires s on s.Id = a.Id
    left join notes t on t.Id = a.Id
    where (b.Principale is null or b.Principale = 1) -- évite les doublons de ligne pour une même photo
        and a.NomFichier is not null  -- exclure de la requête tout ce qui n'est pas numérique (et donc n'a pas de fichier)
"""

#1bis - pour chaque arrondissement, itérer
#2 - csv to pandas df

con = sqlite3.connect(sql_filepath)
log("Exécution de la requête pour créer le dataframe")  
df = pd.read_sql(requete, con)
log("Dataframe créé : " + str(len(df.index)) +  " rows")

#attribution du numéro d'inventaire 
log("Attribution des numéros d'inventaire")
df.insert(0, 'N_inventaire', range(ninv_debut_serie, ninv_debut_serie + len(df)))
df = df.drop('n_inventaire', axis=1)

#3 - nettoyage données dans df + alignement avec référentiels
log("Trimming")
df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

#rue
log("Nettoyage rue")
def nettoyer_rue(string):
    if string:
        result =  re.sub(r"(.*) ((AVENUE|IMPASSE|QUAI|VOIE|RUELLE|PLACE|BOULEVARD|RUE|VOIE|CIT(E|É)|ALL(É|E)E|CHEMIN|ROUTE|GR|R(É|E)SIDENCE|HAMEAU|LIEU(-| )DIT|TRAVERSE|PROMENADE|ROND(-| )POINT|PASSAGE) ?((D((E(S)?)|(U)).*)|(L.*))?)$", r"\1, \2", string.upper())
    else: 
        result = string
    return result
df['rue'] = df['rue'].apply(nettoyer_rue)

#nettoyer site
log("Nettoyage site")
def nettoyer_site(string):
    result = string
    if string:
        string = (re.sub(r"([^\_ \.]+\_)+[^\_ \.]+", "",string)).strip().upper()
        if string != "" and string:
            result = string
        else:
            result = "IMMEUBLE"
    return result
df['site'] = df['site'].apply(nettoyer_site)

#nettoyer arrdt
log("Nettoyage arrondissement")
df['arrondissement'] = df['arrdt'] 
def nettoyer_arrondissement(string):
    result= string
    if string:
        if "Paris" in string:
            result = re.sub(r'[^0-9]', '', string)
        else:
            result = np.nan
    else:
        result = np.nan
    return result
df['arrondissement'] = df['arrondissement'].apply(nettoyer_arrondissement)

# nettoyer ville
log("Nettoyage ville")
df['localite'] = df['arrdt']
def nettoyer_localite(string):
    result = string
    if string: 
        if "Paris" in string:
            result = "PARIS"
        elif  "Vincennes" in string:
            result = "VINCENNES"
        elif "Saint-Denis" in string:
            result = "SAINT-DENIS"
        else:
            result = np.nan
    else:
        result = np.nan
    return result
df['localite'] = df['localite'].apply(nettoyer_localite)

# support
log("Attribution du support")
def support(string):
    return "NUMERIQUE"
df['support']= df['support'].apply(support)

#couleur
log("Nettoyage couleur") 
def nettoyer_couleur(string):
    result = string
    if string: 
        result= string.upper()
    else:
        result  = np.nan
    return result
df['couleur']= df['couleur'].apply(nettoyer_couleur)

#prise de vue
log("Nettoyage date de prise de vue")
def nettoyer_prisevue(string):
    result = np.nan 
    try:
        result = (dateparser.parse(string.replace('-', '/') ).date()).strftime("%Y/%m/%d")
    except:
        if string:
            result = np.nan
        else:
            result = np.nan
    return result
df['date_prise_vue']= df['date_prise_vue'].apply(nettoyer_prisevue)

#nettoayge photographe
log("Nettoyage photographe")
def nettoyer_photographe(string):
    result = string
    if string:
        result = re.sub(r'(.*) ([^ ]+)', r'\2, \1', string).upper()
    else:
        result=string
    return result
df['photographe']= df['photographe'].apply(nettoyer_photographe)

#nettoyage droits
log("Nettoyage droits")
def nettoyer_droits(string):
    result=string
    if string:
        if string in ['Paris historique']:
            result = 'PH'
        elif string in ['Pas Paris historique - ne pas diffuser', 'BHVP', 'Photographe', 'CNMH-SPADEM', 'Voir commentaire']:
            result= 'PHOTOGRAPHE'
        else:
            result = np.nan
    else:
        result = np.nan
    return result
df['droits']= df['droits'].apply(nettoyer_droits)

#nettoyage mh
log("Nettoyage MH")
def nettoyer_mh(string):
    result = np.nan 
    if string:
        if 'Mérimée' == string or 'Mérimée' in string:
            result = 'OUI'
        else:
            result='NON'
    else:
        result = 'INCONNU'
    return result
df['mh']= df['mh'].apply(nettoyer_mh)

#nettoyage généralité
log("Nettoyage généralité d'architecture")
def nettoyer_generalite(string):
    generalites_ref = ['COMMÉMORATIVE_VOTIVE_FUNÉRAIRE','JARDINS_EAUX','INDUSTRIELLE','MILITAIRE','PRIVÉE','PUBLIQUE','VOIRIES_ESPACES_LIBRES']
    result = ''
    if string:
        flag = False
        if 'ARCHITECTURE PRIVÉE' in string.upper()   or 'HOTEL' in string.upper() or 'HÔTEL' in string.upper():
            result += "PRIVÉE/"
            flag = True
        if 'ARCHITECTURE PUBLIQUE' in string.upper() or 'CIVIL' in string.upper() or 'TROPOL' in string.upper() or 'CEINTURE' in string.upper() :
            result += "PUBLIQUE/"
            flag = True
        if 'VOIRIE' in string.upper():
            result+= 'VOIRIES_ESPACES_LIBRES/'
            flag = True
        if 'RELI' in string.upper() or 'VOTIVE' in string.upper():
            result+= 'COMMÉMORATIVE_VOTIVE_FUNÉRAIRE/'
            flag = True
        if 'JARDIN' in string.upper():
            result+= 'JARDINS_EAUX/'
            flag = True
        if 'INDUS' in string.upper():
            result+= 'INDUSTRIELLE/'
            flag = True
        if 'MILI' in string.upper():
            result+= 'MILITAIRE/'
            flag = True
        if flag == False :
            result = np.nan
        else:
            result = re.sub(r'/$', '', result)
    else :
        result = np.nan
    return result
df['generalite']= df['generalite'].apply(nettoyer_generalite)

#mots clés
def nettoyer_m(string):
    if string:
        result = keywords(string).join_with_words_list(liste_mots_cles)
        if len(result) > 0:
            result = result[0]
        else:
            result = np.nan
    else :
        result = np.nan
    return result
for x in range(1,7):
    current = 'm'+str(x)
    log("Nettoyage des mots clés "+current)
    df[current]= df[current].apply(nettoyer_m)

#date d'inventaire
log("Nettoyage de la date d'inventaire")
def nettoyer_date_inventaire(string):
    result=string
    if string:
        try:
            result = (dateparser.parse(string.replace('-', '/') ).date()).strftime("%Y/%m/%d")
        except:
            result = datetime.datetime.now().strftime("%Y/%m/%d")
    else:
        result = datetime.datetime.now().strftime("%Y/%m/%d")
    return result
df['date_inventaire']= df['date_inventaire'].apply(nettoyer_date_inventaire)

#auteur
log("Nettoyage de l'auteur")
def nettoyer_auteur(string):
    result = 'INCONNU'
    if string :
        string = string.replace("BEN MILED", 'BEN-MILED').replace("é", "É").replace("ç", "Ç").replace("è", "È")
        result = re.sub(r'(.*) ([^ ]+)$', r'\2', string).upper()
    if result == 'IDENTIFIÉ':
        result = 'INCONNU'
    return result
df['auteur']= df['auteur'].apply(nettoyer_auteur)

#cote physique
log("Attribution cote physique")
def cote_phy(string):
    return 'NUM'
df['cote_physique']= df['cote_physique'].apply(cote_phys)

#TODO: pour latitude et longitude, aller les chercher en priorité dans la base adresse_cadastre (utiliser
# AdresseParser().compare() pour faire le match), puis lat et lon de la base num prévalent, puis celles
# rempli et déjà dans l'inventaire

#suppressions et renommages
log("Suppression de colonnes et renommages")
df = df.drop('arrdt', axis=1)
df = df.drop('ville', axis=1)
df['ville'] = df['localite']
df = df.drop('localite', axis=1)
df = df[['N_inventaire', 'rue', 'n_rue', 'site', 'arrondissement', 'ville', 'dpt', 'lat', 'lon', 'support', 'couleur', 'taille', 'date_prise_vue', 'photographe', 'droits', 'don', 'collection', 'date_construction', 'architecte', 'mh', 'legende', 'generalite', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'autre_adresse', 'notes', 'cote_base', 'cote_physique', 'date_inventaire', 'auteur']]

log("Conversion en CSV")
df.to_csv(path_or_buf=working_dir+"numerique.csv",index=False, sep='|', header=True, quotechar='"', quoting=csv.QUOTE_ALL,escapechar='"')

log("Fin des traitements")


#ancienne méthode
# créer le csv à partir requete sql: boucler sur les sites afin de faire un csv par arrondissement
# TODO: remove limit and order for complete run and where
'''differents_sites = """
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
                    rue_norm = re.sub(r"(.*) (((COURS)|(RUE)|(PLACE)|(BOULEVARD)|(AVENUE)|(QUAI)|(COUR)) ?((D((E(S)?)|(U)).*)|(L.*))?)$", r"\1, \2", rue.upper())
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
'''