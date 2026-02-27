import pandas as pd
import json
import logging

def audit_dataframe(df, df_name):
    report = {}
    
    report["dataframe_name"] = df_name
    report["nb_rows"] = len(df)
    report["nb_columns"] = len(df.columns)
    report["duplicates"] = int(df.duplicated().sum())
    
    report["columns"] = {}
    
    total_missing_percent = 0
    numeric_columns_checked = 0
    
    for col in df.columns:
        col_data = df[col]
        missing_count = col_data.isnull().sum()
        missing_percent = col_data.isnull().mean() * 100
        
        column_report = {
            "dtype": str(col_data.dtype),
            "missing_values": int(missing_count),
            "missing_percent": round(float(missing_percent), 2),
            "unique_values": int(col_data.nunique())
        }
        
        # Vérification valeurs négatives sur colonnes numériques
        if pd.api.types.is_numeric_dtype(col_data):
            numeric_columns_checked += 1
            negative_values = int((col_data < 0).sum())
            column_report["negative_values"] = negative_values
            
        report["columns"][col] = column_report
        total_missing_percent += missing_percent

    avg_missing = total_missing_percent / len(df.columns) if len(df.columns) > 0 else 0
    duplicate_penalty = report["duplicates"] / len(df) * 100 if len(df) > 0 else 0
    
    quality_score = 100 - avg_missing - duplicate_penalty
    quality_score = max(0, round(quality_score, 2))
    
    report["quality_score"] = quality_score
    
    return report
def audit_all_silver_dataframes(namespace, output_file="data_quality_report.json"):
    reports = []

    for var_name, var_value in namespace.items():
        if var_name.startswith("silver_") and isinstance(var_value, pd.DataFrame):
            print(f"Audit en cours : {var_name}")
            report = audit_dataframe(var_value, var_name)
            reports.append(report)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(reports, f, indent=4, ensure_ascii=False)

    print(f"\nAudit terminé ✅ Rapport sauvegardé dans : {output_file}")
    return reports