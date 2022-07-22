from AdresseParser import AdresseParser
import csv

class Coordonnees():
	def __init__(self, sourcedatapath:str, destination_principale_filter:list = []):
		self.donnees_chargees = []
		self.pour_comparaison = []

		with open(sourcedatapath) as f:
			f_o = csv.reader(f, delimiter=';')
			next(f_o)
			parser = AdresseParser()
			for row in f_o:
				adresse = row[2] +" " + row[5] + " " + row[8].replace("751", "750") + " PARIS" 
				parsee = parser.parse(adresse)
				if len(destination_principale_filter) == 0:
					self.donnees_chargees.append({
						"adresse": parsee , 
						"lat":row[12], 
						"lon":row[11]})
					self.pour_comparaison.append(parsee)
				else:
					if row[7] in destination_principale_filter:
						self.donnees_chargees.append({
						"adresse": parsee, 
						"lat":row[12], 
						"lon":row[11]})	
						self.pour_comparaison.append(parsee)

	def get_from_bano_cadastre(self,adresse:str):
		lat, lon = (None, None)
		donnees_trouvees = self.donnees_chargees[self.pour_comparaison.index(AdresseParser().parse(adresse))]
		lat = donnees_trouvees["lat"]
		lon = donnees_trouvees["lon"]
		return (lat, lon )