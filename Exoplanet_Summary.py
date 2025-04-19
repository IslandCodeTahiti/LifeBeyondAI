import pandas as pd
import numpy as np
from pymongo import MongoClient

# === CONFIGURATION ===
CSV_PATH = "Raw Files/PS_2025.04.19_07.26.50.csv"  # à adapter
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "LifeBeyond"
COLLECTION_NAME = "exoplanet_summary"

# === FONCTION POUR EXTRAIRE LES INCERTITUDES ===
def parse_uncertainty(row, col):
    try:
        val = float(row[col])
        err1 = row.get(col + 'err1', np.nan)
        err2 = row.get(col + 'err2', np.nan)
        val_min = val - abs(float(err2)) if not pd.isna(err2) else val
        val_max = val + abs(float(err1)) if not pd.isna(err1) else val
        return {
            "value_mean": val,
            "value_min": val_min,
            "value_max": val_max
        }
    except (TypeError, ValueError):
        return None

# === CHARGEMENT DES DONNÉES ===
df = pd.read_csv(CSV_PATH, comment='#', encoding='utf-8')
df.columns = df.columns.str.strip()

records = []

for _, row in df.iterrows():
    record = {
        "planet_name": row.get("pl_name"),
        "host_name": row.get("hostname"),
        "num_stars": row.get("sy_snum"),
        "num_planets": row.get("sy_pnum"),
        "discovery": {
            "method": row.get("discoverymethod"),
            "year": row.get("disc_year"),
            "facility": row.get("disc_facility")
        },
        "solution_type": row.get("soltype"),
        "controversial": row.get("pl_controv_flag"),
        "reference": row.get("pl_refname"),
        "orbital_period": parse_uncertainty(row, "pl_orbper"),
        "semi_major_axis": parse_uncertainty(row, "pl_orbsmax"),
        "planet_radius_earth": parse_uncertainty(row, "pl_rade"),
        "planet_radius_jupiter": parse_uncertainty(row, "pl_radj"),
        "planet_mass_earth": parse_uncertainty(row, "pl_bmasse"),
        "planet_mass_jupiter": parse_uncertainty(row, "pl_bmassj"),
        "eccentricity": parse_uncertainty(row, "pl_orbeccen"),
        "insolation_flux": parse_uncertainty(row, "pl_insol"),
        "equilibrium_temperature": parse_uncertainty(row, "pl_eqt"),
        "ttv_flag": row.get("ttv_flag"),
        "stellar": {
            "reference": row.get("st_refname"),
            "spectral_type": row.get("st_spectype"),
            "temperature": parse_uncertainty(row, "st_teff"),
            "radius": parse_uncertainty(row, "st_rad"),
            "mass": parse_uncertainty(row, "st_mass"),
            "metallicity": parse_uncertainty(row, "st_met"),
            "metallicity_ratio": row.get("st_metratio"),
            "gravity": parse_uncertainty(row, "st_logg")
        },
        "system_reference": row.get("sy_refname"),
        "position": {
            "ra": row.get("rastr"),
            "dec": row.get("decstr"),
            "distance_pc": parse_uncertainty(row, "sy_dist")
        },
        "magnitudes": {
            "v": parse_uncertainty(row, "sy_vmag"),
            "k": parse_uncertainty(row, "sy_kmag"),
            "gaia": parse_uncertainty(row, "sy_gaiamag")
        },
        "update_info": {
            "last_update": row.get("rowupdate"),
            "publication_date": row.get("pl_pubdate"),
            "release_date": row.get("releasedate")
        }
    }
    records.append(record)

# === INSERTION DANS MONGODB ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.insert_many(records)
print(f"Inserted {len(records)} records into MongoDB.")
