# data_analytics.py
#!/usr/bin/python

import sys
import pandas as pd

from db_utils import connectToPostgres

host = sys.argv[1] 
database = sys.argv[2] 
user = sys.argv[3]
password = sys.argv[4] 
port = sys.argv[5] 

engine, error_message = connectToPostgres(host=host, port=port, database=database, user=user, password=password)

def summarise(df, key, pivot_column, value_column, function):
    if function == 'mean':
        dasbor = df.groupby(pivot_column)[[value_column]].mean().reset_index()
        val = dasbor[value_column].to_list()
        val.append(df[value_column].mean())
        val = [ round(elem, 2) for elem in val ]
    
    if function == 'sum':
        dasbor = df.groupby(pivot_column)[[value_column]].sum().reset_index()
        val = dasbor[value_column].to_list()
        val.append(df[value_column].sum())
        val = [ round(elem, 2) for elem in val ]

    if function == 'count':
        dasbor = df.groupby(pivot_column)[[value_column]].count().reset_index()
        val = dasbor[value_column].to_list()
        val.append(df[value_column].count())

    if function == 'unique':
        dasbor = df.groupby(pivot_column)[[value_column]].nunique().reset_index()
        val = dasbor[value_column].to_list()
        val.append(df[value_column].nunique())

    district_id = dasbor[pivot_column].to_list()
    district_id.append("")
    dict = {'district_id': district_id, 'value': val} 
    dasbor_all = pd.DataFrame(dict)
    n = len(dasbor_all)
    dasbor_all.insert(0, "id", [i for i in range(n)], True)
    dasbor_all.insert(2, "key", [key for i in range(n)], True)
    dasbor_all.insert(3, "category", ["" for i in range(n)], True)
    dasbor_all["created_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    dasbor_all["updated_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    return(dasbor_all)

# q1 = "SELECT * FROM farmers;"
# farmers = pd.read_sql_query(q1, engine)
q2 = "SELECT * FROM farms;"
farms = pd.read_sql_query(q2, engine)
q3 = "SELECT * FROM farm_mills;"
farm_mills = pd.read_sql_query(q3, engine)
q4 = "SELECT id, district_id FROM mills;"
mills = pd.read_sql_query(q4, engine)
farm_mills.drop(columns="id", inplace=True)

df_farms = farms.copy()

df_merged = farm_mills.merge(df_farms[["id", "district_id"]], left_on="farm_id", right_on="id", how="left").drop(columns=["created_at", "updated_at"])
df_merged = df_merged[["farm_id", "mill_id", "district_id"]].merge(mills, left_on=["mill_id", "district_id"], right_on=["id", "district_id"], how="left")

dasbor_all_luas_kebun = summarise(df_farms, 'plant_area', 'district_id', 'plant_areaDoc', 'sum')
dasbor_all_produksi_tanaman = summarise(df_farms, 'plant_production', 'district_id', 'plant_production', 'sum')
dasbor_all_produktifitas_tanaman = summarise(df_farms, 'plant_productivity', 'district_id', 'plant_productivity', 'mean')
dasbor_all_farmer = summarise(df_farms, 'smallholder_farmer', 'district_id', 'plant_productivity', 'count')
dasbor_all_plant_age = summarise(df_farms, 'plant_age', 'district_id', 'plant_age', 'mean')
dasbor_all_mill = summarise(df_merged, 'mills_linked', 'district_id', 'mill_id', 'unique')

df_list = [dasbor_all_luas_kebun, dasbor_all_produksi_tanaman, dasbor_all_farmer, dasbor_all_produktifitas_tanaman, dasbor_all_plant_age, dasbor_all_mill]
res = pd.concat(df_list, ignore_index=True)
res['id'] = [i+1 for i in range(len(res))]
res.to_sql('data_analytics', engine, if_exists='replace', index=False)