import pandas as pd
import re
from pymongo import MongoClient
import numpy as np

# === CONFIGURATION ===
CSV_PATH = "Raw Files/PSCompPars_2025.04.19_07.32.18.csv"  # à adapter
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "LifeBeyond"
COLLECTION_NAME = "exoplanet_parameters"

# === FONCTIONS DE PARSING ===
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

# === CHARGEMENT ET NETTOYAGE ===
df = pd.read_csv(CSV_PATH, comment='#', encoding='utf-8')
df.columns = df.columns.str.strip()

records = []

for _, row in df.iterrows():
    record = {
        "planet_name": row.get("pl_name"),
        "host_name": row.get("hostname"),
        "tic_id": row.get("tic_id"),
        "gaia_id": row.get("gaia_id"),
        "orbital_period": parse_uncertainty(row, "pl_orbper"),
        "semi_major_axis": parse_uncertainty(row, "pl_orbsmax"),
        "planet_radius_earth": parse_uncertainty(row, "pl_rade"),
        "planet_radius_jupiter": parse_uncertainty(row, "pl_radj"),
        "planet_mass_earth": parse_uncertainty(row, "pl_bmasse"),
        "planet_mass_jupiter": parse_uncertainty(row, "pl_bmassj"),
        "planet_density": parse_uncertainty(row, "pl_dens"),
        "eccentricity": parse_uncertainty(row, "pl_orbeccen"),
        "insolation_flux": parse_uncertainty(row, "pl_insol"),
        "equilibrium_temperature": parse_uncertainty(row, "pl_eqt"),
        "inclination": parse_uncertainty(row, "pl_orbincl"),
        "transit_midpoint": parse_uncertainty(row, "pl_tranmid"),
        "impact_parameter": parse_uncertainty(row, "pl_imppar"),
        "transit_depth": parse_uncertainty(row, "pl_trandep"),
        "transit_duration": parse_uncertainty(row, "pl_trandur"),
        "stellar_temperature": parse_uncertainty(row, "st_teff"),
        "stellar_radius": parse_uncertainty(row, "st_rad"),
        "stellar_mass": parse_uncertainty(row, "st_mass"),
        "stellar_metallicity": parse_uncertainty(row, "st_met"),
        "stellar_gravity": parse_uncertainty(row, "st_logg"),
        "stellar_age": parse_uncertainty(row, "st_age"),
        "stellar_density": parse_uncertainty(row, "st_dens"),
        "stellar_luminosity": parse_uncertainty(row, "st_lum"),
        "stellar_rotation_velocity": parse_uncertainty(row, "st_vsin"),
        "stellar_rotation_period": parse_uncertainty(row, "st_rotp"),
        "system_distance_pc": parse_uncertainty(row, "sy_dist"),
        "system_pm": parse_uncertainty(row, "sy_pm"),
        "system_ra": row.get("rastr"),
        "system_dec": row.get("decstr"),
        "magnitudes": {
            "v_mag": parse_uncertainty(row, "sy_vmag"),
            "j_mag": parse_uncertainty(row, "sy_jmag"),
            "h_mag": parse_uncertainty(row, "sy_hmag"),
            "k_mag": parse_uncertainty(row, "sy_kmag"),
            "g_mag": parse_uncertainty(row, "sy_gmag"),
            "tess_mag": parse_uncertainty(row, "sy_tmag"),
            "gaia_mag": parse_uncertainty(row, "sy_gaiamag")
        }
    }
    records.append(record)

# === INSERTION MONGODB ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.insert_many(records)
print(f"Inserted {len(records)} planetary records into MongoDB.")
