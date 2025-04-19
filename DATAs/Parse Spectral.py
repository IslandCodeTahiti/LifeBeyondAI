import os
import pandas as pd
from pymongo import MongoClient

# === CONFIGURATION ===
TBL_DIR = "Spectral"  # dossier contenant les fichiers .tbl
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "LifeBeyond"
COLLECTION_NAME = "spectral"

# === CONNEXION MONGODB ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# === TRAITEMENT DE TOUS LES FICHIERS .tbl ===
for filename in os.listdir(TBL_DIR):
    if filename.endswith(".tbl"):
        path = os.path.join(TBL_DIR, filename)
        print(f"Traitement de {filename}...")

        metadata = {}
        columns = []
        spectra = []

        with open(path, "r") as f:
            lines = f.readlines()

        for line in lines:
            if line.startswith("\\") and "=" in line:
                key, val = line.strip("\\\n").split("=", 1)
                metadata[key.strip()] = val.strip()
            elif line.startswith("|") and not columns:
                columns = [c.strip() for c in line.strip().split("|") if c.strip()]
            elif not line.startswith("\\") and not line.startswith("|") and line.strip():
                parts = line.strip().split()
                if len(parts) == len(columns):
                    row = dict(zip(columns, parts))
                    spectra.append(row)

        if spectra:
            observation = {
                "_source_file": filename,
                "metadata": metadata,
                "spectra": spectra
            }
            collection.insert_one(observation)
            print(f"Inséré {len(spectra)} points spectraux depuis {filename} en une seule observation.")
        else:
            print(f"Aucune donnée trouvée dans {filename}.")# === CONNEXION MONGODB ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# === TRAITEMENT DE TOUS LES FICHIERS .tbl ===
for filename in os.listdir(TBL_DIR):
    if filename.endswith(".tbl"):
        path = os.path.join(TBL_DIR, filename)
        print(f"Traitement de {filename}...")

        metadata = {}
        columns = []
        spectra = []

        with open(path, "r") as f:
            lines = f.readlines()

        for line in lines:
            if line.startswith("\\") and "=" in line:
                key, val = line.strip("\\\n").split("=", 1)
                metadata[key.strip()] = val.strip()
            elif line.startswith("|") and not columns:
                columns = [c.strip() for c in line.strip().split("|") if c.strip()]
            elif not line.startswith("\\") and not line.startswith("|") and line.strip():
                parts = line.strip().split()
                if len(parts) == len(columns):
                    row = dict(zip(columns, parts))
                    spectra.append(row)

        if spectra:
            observation = {
                "_source_file": filename,
                "metadata": metadata,
                "spectra": spectra
            }
            collection.insert_one(observation)
            print(f"Inséré {len(spectra)} points spectraux depuis {filename} en une seule observation.")
        else:
            print(f"Aucune donnée trouvée dans {filename}.")
