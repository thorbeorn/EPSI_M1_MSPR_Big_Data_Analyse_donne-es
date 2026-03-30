import os
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Chargement du module de la couche IA
load_module = getattr(__import__("[ia]prediction.load"), "load")

# Chargement des données Gold
print("Chargement du dataset all_indicator.parquet...")
df_indicator = load_module.load_parquet_from_minio("all_indicator.parquet")
print(f"Dataset chargé : {len(df_indicator)} lignes, {len(df_indicator.columns)} colonnes")

# Informations de début
print("\n=== Aperçu des données ===")
print(df_indicator.head())
print("\nInfos colonnes :")
print(df_indicator.info())
print("\nStatistiques descriptives :")
print(df_indicator.describe(include='all'))

# Création du répertoire de sortie des figures
output_dir = os.path.join(os.path.dirname(__file__), "eda_outputs")
os.makedirs(output_dir, exist_ok=True)

# Fonction pour nettoyer les noms de fichiers
import re
def sanitize_filename(name):
    return re.sub(r'[<>:"|?*\\/]', '_', name)

# 1) Analyse descriptive des variables (histogrammes)
num_cols = df_indicator.select_dtypes(include=[np.number]).columns.tolist()
print(f"\nColonnes numériques identifiées ({len(num_cols)}) : {num_cols}")

for col in num_cols:
    plt.figure(figsize=(8, 4))
    df_indicator[col].hist(bins=50, color="dodgerblue", edgecolor="black")
    plt.title(f"Histogramme de {col}")
    plt.xlabel(col)
    plt.ylabel("Fréquence")
    plt.tight_layout()
    safe_col = sanitize_filename(col)
    plt.savefig(os.path.join(output_dir, f"hist_{safe_col}.png"))
    plt.close()

# 2) Corrélations + heatmap
corr = df_indicator[num_cols].corr()
plt.figure(figsize=(14, 12))
plt.title("Matrice de corrélation")
heatmap = plt.imshow(corr, cmap="RdBu", vmin=-1, vmax=1)
plt.colorbar(heatmap, fraction=0.046, pad=0.04)
plt.xticks(range(len(num_cols)), num_cols, rotation=90)
plt.yticks(range(len(num_cols)), num_cols)
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "corr_heatmap.png"))
plt.close()

# 3) Analyse temporelle (si colonne année existe)
if "annee" in df_indicator.columns:
    time_cols = [c for c in num_cols if c not in ["code_dept", "dept", "annee"]]
    subsidiaries = df_indicator.groupby("annee")[time_cols].mean().reset_index()
    for col in time_cols:
        plt.figure(figsize=(10, 4))
        plt.plot(subsidiaries["annee"], subsidiaries[col], marker="o")
        plt.title(f"Évolution temporelle moyenne de {col}")
        plt.xlabel("Année")
        plt.ylabel(col)
        plt.grid(True)
        plt.tight_layout()
        safe_col = sanitize_filename(col)
        plt.savefig(os.path.join(output_dir, f"time_{safe_col}.png"))
        plt.close()

# 4) Analyse spatiale simple (si code département existe)
dept_col_candidates = ["code_dept", "dept", "departement", "Code_departement"]
dept_col = None
for cand in dept_col_candidates:
    if cand in df_indicator.columns:
        dept_col = cand
        break

if dept_col:
    print(f"Analyse spatiale : colonne département trouvée : {dept_col}")
    spatial_cols = time_cols  # Toutes les colonnes temporelles
    for col in spatial_cols:
        if col in df_indicator.columns:
            plt.figure(figsize=(10, 4))
            top = df_indicator.groupby(dept_col)[col].mean().sort_values(ascending=False).head(25)
            top.plot(kind="bar", color="seagreen")
            plt.title(f"Top 25 départements pour {col}")
            plt.xlabel(dept_col)
            plt.ylabel(col)
            plt.xticks(rotation=45)
            plt.tight_layout()
            safe_col = sanitize_filename(col)
            plt.savefig(os.path.join(output_dir, f"spatial_{safe_col}.png"))
            plt.close()
else:
    print("Analyse spatiale : aucune colonne département trouvée (code_dept, dept, departement, code_departement)")

print("\nEDA terminée. Figures enregistrées dans :", output_dir)
