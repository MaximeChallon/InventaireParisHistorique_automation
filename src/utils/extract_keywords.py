import nltk
from rake_nltk import Rake
import re
from sqlalchemy import create_engine

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

