import logging
from datetime import datetime

# CONFIGURATION DU LOGGING
logger = logging.getLogger(__name__)

load_module = getattr(__import__("[ia]prediction.load"), "load")
data_quality_module = getattr(__import__("[ia]prediction.data_quality"), "data_quality")

RandomForest_GradientBoosting = getattr(__import__("[ia]prediction.RandomForest_GradientBoosting"), "RandomForest_GradientBoosting")

FILES = {
    "all_indicator": "all_indicator.parquet",
    "all_president": "all_president.parquet"
}

df_indicator = load_module.load_parquet_from_minio(FILES["all_indicator"])
data_quality_module.quality_report(df_indicator, "df_indicator", f"indicator_quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json")
df_president = load_module.load_parquet_from_minio(FILES["all_president"])
data_quality_module.quality_report(df_president, "df_president", f"president_quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json")

RandomForest_GradientBoosting.train_and_predict(df_indicator, df_president, annee_cible=2024)