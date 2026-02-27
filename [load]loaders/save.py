from sqlalchemy import create_engine
import pandas as pd
import logging

engine = create_engine(
    "mysql+pymysql://mspr-user:z9k5RYgeDr3457TV33tY2eLPgd36XE5y88LAcCpz@localhost:3306/mspr-db"
)

def save_all_silver_dataframes(dfs):
    for var_name, var_value in dfs.items():
        if var_name.startswith("silver_") and isinstance(var_value, pd.DataFrame):
            print(f"Save en cours : {var_name}")
            var_value.to_sql(
                name=str(var_name).removeprefix("silver_").removesuffix("_df"),
                con=engine,
                if_exists="replace",
                index=False
            )

    print(f"\save terminé ✅")