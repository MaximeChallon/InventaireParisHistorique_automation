import pandas_access as mdb
import pandas as pd
import os
import sqlite3
import subprocess 
import re
import csv

#constantes
"""
filepath_entree = "/home/maxime/Téléchargements/Phototheque.mdb"
filepath_out = "/home/maxime/Téléchargements/Phototheque.sqlite"
filepath_out_csv = "/home/maxime/Téléchargements/Phototheque.csv"
filepath_out_sql = "/home/maxime/Téléchargements/Phototheque.sql"
"""

try:
    os.system("rm "+ filepath_out)
except:
    pass

try:
    os.system("rm "+ filepath_out_csv)
except:
    pass

try:
    os.system("rm "+ filepath_out)
except:
    pass

class Mdb():
    def __init__(self, filepath):
        self.filepath = filepath

    def _get_tables(self):
        return [table for table in mdb.list_tables(self.filepath)]

    def _get_csv(self, table, filepath_out_csv):
        os.system("mdb-export " + self.filepath + " " + table + " --> "  + filepath_out_csv)
        return None

    def _get_insert(self, table, filepath_out_sql):
        os.system("mdb-export -I sqlite " + self.filepath + " " + table + " --> " + filepath_out_sql)
        return None

    def _to_df_from_csv(self, filepath_out_csv):
        df = pd.read_csv(filepath_out_csv, low_memory=False)
        return df

    def _request_to_csv(self, request, filepath_out_csv, sqlite_filepath):
        conn = sqlite3.connect(sqlite_filepath, isolation_level=None,
                       detect_types=sqlite3.PARSE_COLNAMES)
        db_df = pd.read_sql_query(request, conn)
        db_df.to_csv(filepath_out_csv, index=False, sep="|", quoting=csv.QUOTE_ALL)
        conn.close()

    def _to_sqlite(self, outpath):
        # prend une vingtaine de mn
        try:
            os.system("rm "+ outpath)
        except:
            pass
        if not (os.path.exists(outpath)):
            open(outpath, "w").close()    
        conn = sqlite3.connect(outpath)  
        curs = conn.cursor()
        # créer les tables
        tables_crees = []
        for table in self._get_tables():
            if table not in tables_crees :
                print (table)
                create_instruction = subprocess.check_output('mdb-schema -T "'+table+'" '+self.filepath+' sqlite', shell=True)
                curs.execute(create_instruction.decode('utf-8'))
                conn.commit()
                tables_crees.append(table)
                # insérer les données des tables que si la table existe 
                try:
                    insert_instructions = subprocess.check_output('mdb-export -I sqlite  '+self.filepath+' "'+table+'" ', shell=True)
                    rang_liste = 0
                    n_p = 0
                    insert_instructions = re.sub("\n|\r|\n\r|\r\n", "", (insert_instructions.decode('utf-8'))).split(";INSERT")
                    inserts  = re.sub("\) +VALUES.*", ") ", insert_instructions[0]) + " values "
                    for insert in insert_instructions:
                        if n_p < 1000:
                            inserts = inserts + re.sub( ".* ?VALUES", " ", insert_instructions[rang_liste]) + ","
                            rang_liste += 1
                            n_p += 1
                        else:
                            inserts = inserts + re.sub( ".* ?VALUES", " ", insert_instructions[rang_liste]) + ","
                            n_p  = 0
                            print(str(rang_liste) + " items")
                            rang_liste += 1
                            curs.execute(re.sub("\n|\r|\n\r|\r\n", " ", re.sub(",$", "", inserts)) + ";")
                            conn.commit()
                            inserts  = re.sub("\) +VALUES.*", ") ", insert_instructions[0]) + " values "
                    if n_p != 0:
                        curs.execute(re.sub(",$", "",re.sub("\n|\r|\n\r|\r\n", " ", re.sub(",$", "", inserts)) ))
                        conn.commit()
                except Exception as e:
                    print(e)
                conn.commit()
        conn.close() 

#entree = Mdb(filepath_entree)
#print(entree._get_tables())
#entree._get_csv(table="Photos", filepath_out_csv=filepath_out_csv)
#print(entree._to_df( filepath_out_csv))

#entree._to_sqlite(filepath_out, filepath_out_csv)
#print(entree._get_insert(table="Photos", filepath_out_sql=filepath_out_sql))