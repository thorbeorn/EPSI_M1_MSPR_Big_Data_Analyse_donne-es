import pandas as pd
import json
import requests
import logging
import urllib3

# Set le logger pour les logs
logger = logging.getLogger(__name__)
# Desactivation du warning externe pour le certificat
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
Charge un dataframe depuis l'API melodi

Parameters
----------
melodi_url : str
		URL de l'API de melodi

Returns
-------
pd.DataFrame
"""
def creer_dataframe_depuis_melodi_api_url(melodi_url: str) -> pd.DataFrame:
	logger.info("Telechargement des données depuis l'API MELODI")
	
	try:
		# Telechargement des données depuis l'API
		logger.debug("Telechargement des données depuis l'API MELODI avec request")
		get_data = requests.get(melodi_url, verify=False)
		data_from_net = get_data.content
		data = json.loads(data_from_net)

		# Extraction des informations du jeu de données
		logger.debug("Extraction des informations du jeu de données")
		title = data['title']['fr']
		identifier = data['identifier']

		#Extraction des observations du jeu de données filtré, sur lesquelles on va boucler
		logger.debug("Extraction des observations du jeu de données filtré")
		observations = data['observations']
		extracted_data = []

		#Boucle de lecture des observations dans le json 
		logger.debug("Boucle de lecture des observations")
		for obs in observations:
			dimensions = obs['dimensions']
			
			# Suivant les jeux de données attributes est présent ou non
			logger.debug("On recherche les attribue")
			if 'attributes' in obs:
				attributes = obs['attributes']
			else:
				attributes = None

			# Suivant les jeux de données value peut être absent
			logger.debug("On recherche les valeurs")
			if 'value' in obs['measures']['OBS_VALUE_NIVEAU']:
				print("value")
				measures = obs['measures']['OBS_VALUE_NIVEAU']['value']
			else:
				measures = None
			
			# on rassemble tout dans un objet
			logger.debug("On rassemble tout dans un objet")
			if 'attributes' in obs:
				combined_data = {**dimensions,**attributes, 'OBS_VALUE_NIVEAU': measures}
			else:
				combined_data = {**dimensions, 'OBS_VALUE_NIVEAU': measures}
			
			# On ajoute les data combiné pour en faire un tableau
			logger.debug("On aggrege les données pour avoir un dataframe")
			extracted_data.append(combined_data)

		#Création d'un dataframe pandas
		logger.debug("Création d'un dataframe pandas")
		df = pd.DataFrame(extracted_data)
		logger.info("Retourne le dataframe depuis l'API MELODI")
		return df
	except Exception as e:
		#On raise une erreur et on la LOG
		logger.error("Une erreur est survenue dans l'interogation de l'API MELODI")
		raise Exception(e)