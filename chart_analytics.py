import sys
import json
import pandas as pd
import numpy as np

from db_utils import connectToPostgres

host = sys.argv[1] 
database = sys.argv[2] 
user = sys.argv[3]
password = sys.argv[4] 
port = sys.argv[5] 

engine, error_message = connectToPostgres(host=host, port=port, database=database, user=user, password=password)

def pivot(df, key, column):
    color = ["#d8ff93","#577d0c","#6b8900","#c3da74"]
    dasbor = df[[column]].value_counts().reset_index()
    dasbor = dasbor.sort_values(by=column)
    dasbor['backgroundColor'] = color

    json_value = {
        "label": dasbor[column].to_list(),
        "data": dasbor['count'].to_list(),
        "backgroundColor": dasbor['backgroundColor'].to_list()
    }

    dict = {'district_id': [""], 'value': json.dumps(json_value)} 
    dasbor_all = pd.DataFrame(dict)

    dasbor_per_group = df.groupby(['district_id'])[[column]].value_counts().reset_index()

    result = {}
    df_list = []
    for district in set(dasbor_per_group['district_id']):
        df_filter = dasbor_per_group[dasbor_per_group['district_id'] == district]
        df_filter = df_filter.sort_values(by=column)
        result[district] = {
            "label": df_filter[column].to_list(),
            "data": df_filter['count'].to_list(),
            "backgroundColor": color
        }
        dict = {'district_id': [district], 'value': json.dumps(result[district])} 
        df_list.append(pd.DataFrame(dict))

    df_list.append(dasbor_all)    
    res = pd.concat(df_list, ignore_index=True)

    n = len(res)
    res.insert(0, "id", [i for i in range(n)], True)
    res.insert(2, "key", [key for i in range(n)], True)
    res.insert(3, "category", ["" for i in range(n)], True)
    res["created_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    res["updated_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    return(res)

q1 = "SELECT * FROM farmers;"
farmers = pd.read_sql_query(q1, engine)
df_farmers = farmers.copy()
q2 = "SELECT * FROM farms;"
farms = pd.read_sql_query(q2, engine)
df_farms = farms.copy()

land_legality = pivot(df_farms, 'plant_doc', 'plant_doc')
seed_cert = pivot(df_farms, 'seed_cert', 'seedcert')

dasbor_permit = df_farmers[['permit']].value_counts().reset_index()
dasbor_permit['label'] = np.where(dasbor_permit['permit'].str.contains('doesnthave'), 'No', 'Yes')
dasbor_permit_gb = dasbor_permit.groupby('label')['count'].sum().reset_index()
color = ["#577d0c","#d8ff93"]
dasbor_permit_gb['backgroundColor'] = color
json_value = {
    "label": dasbor_permit_gb['label'].to_list(),
    "data": dasbor_permit_gb['count'].to_list(),
    "backgroundColor": dasbor_permit_gb['backgroundColor'].to_list()
}
dict = {'district_id': [""], 'value': json.dumps(json_value)} 
permit_all = pd.DataFrame(dict)

dasbor_permit_by_district = df_farmers.groupby('district_id')[['permit']].value_counts().reset_index()
dasbor_permit_by_district['label'] = np.where(dasbor_permit_by_district['permit'].str.contains('doesnthave'), 'No', 'Yes')
dasbor_permit_by_district_gb = dasbor_permit_by_district.groupby(['district_id', 'label'])['count'].sum().reset_index()

result = {}
df_list = []
for district in set(dasbor_permit_by_district_gb['district_id']):
    df_filter = dasbor_permit_by_district_gb[dasbor_permit_by_district_gb['district_id'] == district]
    result[district] = {
        "label": df_filter['label'].to_list(),
        "data": df_filter['count'].to_list(),
        "backgroundColor": color
    }
    dict = {'district_id': [district], 'value': json.dumps(result[district])} 
    df_list.append(pd.DataFrame(dict))

df_list.append(permit_all)    
stdb_reg = pd.concat(df_list, ignore_index=True)

n = len(stdb_reg)
stdb_reg.insert(0, "id", [i for i in range(n)], True)
stdb_reg.insert(2, "key", ["stdb_reg" for i in range(n)], True)
stdb_reg.insert(3, "category", ["" for i in range(n)], True)
stdb_reg["created_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
stdb_reg["updated_at"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

df_all = [land_legality, stdb_reg, seed_cert]
chart_analytics = pd.concat(df_all, ignore_index=True)
chart_analytics['id'] = [i+1 for i in range(len(chart_analytics))]
chart_analytics.to_sql('chart_analytics', engine, if_exists='replace', index=False)