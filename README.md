Processus de création de la base finale
* créer une base vide db_finale.sqlite dans InventaireParisHistorique_services/app/
* y lancer le script SQL de création des tables de InventaireParisHistorique_documentation/modelisation/
* Remplir les référentiels: lancer InventaireParisHistorique_services/app/utils/fill_concept.py
* Remplir les photos issues du fonds physique: lancer InventaireParisHistorique_automation/fonds_physique/csv_to_dbfinale.py 
* Remplir les photos issues du fonds numérique: lancer InventaireParisHistorique_automation/fonds_numerique/basenum_to_dbfinale.py

TODO:
* fonds numérique: 
    *  depuis base num
    * depuis groupe sauvegarde
* indexation automatique IA avec InventaireParisHistorique_ai


* créer un service de cotes et d'attribution de nouveaux numéros: OK avec create_inventory_number pour les num d'inventaire, et /insert/num_inv avec body post pour modif la cote