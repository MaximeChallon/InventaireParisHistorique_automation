import requests
import csv
import os
import dotenv
import json
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))

DEBUG = os.environ['DEBUG']
KEY_WS = os.environ["KEY_WS"]
URL_ROOT  = os.environ["URL_ROOT"]
try:
    os.system("rm " + BASE_DIR + "/num_error.csv")
except:
    pass

csv_path = "/home/maxime/dev/InventaireParisHistorique_files/exports/Inventaire_general_phototheque.csv"
max = 100
min = 4500

with open(csv_path, 'r') as f:
    f_o = csv.reader(f, delimiter = '|')
    next(f_o)
    i = 0
    ok = 0
    ko = 0
    l = 0
    for line in f_o:
        # récupérer les numéros traités
        if line[33] and i <= max and l>= min:
            post_headers = {"ws-key": KEY_WS}
            post_data = {"type": "PHOTO"}
            if line[1]:
                post_data["Rue"] = line[1]
            if line[2]:
                post_data["N_rue"] = line[2]
            if line[3]:
                post_data["Site"] = line[3]
            if line[4]:
                post_data["Arrondissement"] = line[4]
            if line[5]:
                post_data["Ville"] = line[5]
            if line[6]:
                post_data["Departement"] = line[6]
            if line[7]:
                post_data["Latitude"] = line[7]
            if line[8]:
                post_data["Longitude"] = line[8]
            if line[9]:
                post_data["Support"] = line[9]
            if line[10]:
                post_data["Couleur"] = line[10]
            if line[11]:
                post_data["Taille"] = line[11]
            if line[12]:
                post_data["Prise_vue"] = line[12]
            if line[13]:
                post_data["Photographe"] = line[13]
            if line[15]:
                post_data["Don"] = line[15]
            if line[16]:
                post_data["Collection"] = line[16]
            if line[17]:
                post_data["Construction"] = line[17]
            if line[18]:
                post_data["Architecte"] = line[18]
            if line[19]:
                post_data["MH"] = line[19]
            if line[20]:
                post_data["Legende"] = line[20]
            if line[21]:
                post_data["Generalite"] = line[21]
            if line[22] or line[23] or line[24] or line[25] or line[26] or line[27]:
                liste_mots = []
                if line[22]:
                    liste_mots.append(line[22])
                if line[23]:
                    liste_mots.append(line[23])
                if line[24]:
                    liste_mots.append(line[24])
                if line[25]:
                    liste_mots.append(line[25])
                if line[26]:
                    liste_mots.append(line[26])
                if line[27]:
                    liste_mots.append(line[27])
                post_data["Mots_cles"] = str(liste_mots).replace("\"", "'")
            if line[28]:
                post_data["Autre_adresse"] = line[28]
            if line[29]:
                post_data["Note"] = line[29]
            if line[30]:
                post_data["Cote_base"] = line[30]
            if line[31]:
                post_data["Cote"] = line[31]
            if line[32] and line[33]:
                post_data["Date_inventaire"] = line[32]
                post_data["Auteur"] = line[33]
            r = requests.post(URL_ROOT + "/insert/" + str(line[0]), data=json.dumps(post_data), headers=post_headers)
            i+=1
            if r.status_code > 400:
                ko += 1
                # mettre le num d'inv en mémoire quelque part avec son message
                with open(BASE_DIR + "/num_error.csv", "a") as f:
                    f_o = csv.writer(f)
                    f_o.writerow([line[0]])
            else:
                ok +=1
            
            sys.stdout.write("\r" + "Process " + str(line[0]) + " -- OK : "+str(ok) +" -- KO : "+str(ko))
            sys.stdout.flush()
        l += 1
    print("\n")