import pandas as pd
from pymongo import MongoClient
import json
import requests

# === CONFIGURATION ===
API_URL = "https://api.le-systeme-solaire.net/rest/bodies/"
CSV_PATH = "solar_system_bodies.csv"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "LifeBeyond"
COLLECTION_NAME = "solar_system_bodies"

# === RÉCUPÉRATION DES DONNÉES VIA L'API ===
response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data['bodies'])
    df.to_csv(CSV_PATH, index=False)
    print(f"Data fetched and saved to {CSV_PATH}")
else:
    raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

# === LECTURE DU FICHIER CSV ===
df = pd.read_csv(CSV_PATH)

# Supprimer les colonnes 'id' et 'name'
df = df.drop(columns=['id', 'name'], errors='ignore')

# Renommer 'englishName' en 'name'
if 'englishName' in df.columns:
    df = df.rename(columns={'englishName': 'name'})

# Remplacement des valeurs NaN par None pour compatibilité MongoDB
df = df.where(pd.notnull(df), None)

# Vérification et conversion des colonnes contenant des dicts
def try_parse_dict(val):
    if isinstance(val, str) and val.strip().startswith("{"):
        try:
            return json.loads(val.replace("'", '"'))  # Corriger les guillemets simples
        except json.JSONDecodeError:
            return val
    return val

for col in ['mass', 'vol', 'aroundPlanet']:
    if col in df.columns:
        df[col] = df[col].apply(try_parse_dict)

# === INSERTION DANS MONGODB ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

records = df.to_dict(orient="records")
collection.insert_many(records)
print(f"Inserted {len(records)} solar system body records into MongoDB.")

