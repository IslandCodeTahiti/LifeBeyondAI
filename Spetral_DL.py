import os
import requests

# === CONFIGURATION ===
BAT_FILE = "Raw Files/wget_atmospheres.bat.txt"  # fichier contenant les URLs
DOWNLOAD_DIR = "Spectral"

# === CRÉATION DU DOSSIER DE TÉLÉCHARGEMENT ===
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === PARSEUR DU FICHIER .bat ===
with open(BAT_FILE, 'r') as f:
    lines = f.readlines()

urls = [line.strip().split(" ")[-1].strip() for line in lines if line.strip().startswith("wget")]

for url in urls:
    filename = url.split("/")[-1]
    local_path = os.path.join(DOWNLOAD_DIR, filename)

    # Télécharger le fichier si non déjà présent
    if not os.path.exists(local_path):
        print(f"Téléchargement de {url}...")
        try:
            r = requests.get(url)
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                f.write(r.content)
            print(f"Fichier téléchargé : {filename}")
        except Exception as e:
            print(f"Erreur lors du téléchargement de {url} : {e}")