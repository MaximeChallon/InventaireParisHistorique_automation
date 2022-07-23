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
annee_max_migration = 2021
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
def cote_phys(string):
    return 'NUM'
df['cote_physique']= df['cote_physique'].apply(cote_phys)

#filtrer pour ne garder que ce qui a une date d'inventaire inférieure à l'année donnée
log("Filtrage des données (avant "+str(annee_max_migration)+")")
df  = df[pd.to_datetime(df['date_inventaire']).dt.year <= annee_max_migration]

#attribution du numéro d'inventaire 
log("Attribution des numéros d'inventaire")
df.insert(0, 'N_inventaire', range(ninv_debut_serie, ninv_debut_serie + len(df)))
df = df.drop('n_inventaire', axis=1)

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