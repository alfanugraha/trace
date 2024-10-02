import json
import pandas as pd
import numpy as np
import geopandas as gpd

from shapely.geometry import Polygon
from db_utils import connectToPostgres

host = sys.argv[1] 
database = sys.argv[2] 
user = sys.argv[3]
password = sys.argv[4] 
port = sys.argv[5] 
data = sys.argv[6]

engine, error_message = connectToPostgres(host=host, port=port, database=database, user=user, password=password)

def combine(large_poly, long_list, lat_list):
    polygon_geom = Polygon(zip(lon_point_list, lat_point_list))
    polygon = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon_geom]) 
    
    # Ensure same CRS
    large_poly = large_poly.to_crs(polygon.crs)
    # Explode the multi-polygon
    large_poly = large_poly.explode(index_parts=False)
    # Spatial intersection
    gdf_intersection = gpd.overlay(large_poly, polygon, how='intersection')
    return(gdf_intersection)
    
q2 = "SELECT * FROM farms;"
farms = pd.read_sql_query(q2, engine)
df_farms = farms.copy()

designation = gpd.read_file(data)
df_spatial = df_farms[['ownerName', 'district_id', 'plant_point', 'plant_polygon', 'plant_polyAreaM2', 'plant_polyAreaHa']]

classes = []
area_m2 = []
area_ha = []
total_overlap_area_m2 = []
total_overlap_area_ha = []
for i in range(df_spatial.shape[0]):
    lon_point_list = []
    lat_point_list = []
    segments = df_spatial['plant_polygon'][i].split(';')
    for segment in segments:
        values = segment.strip().split()  # Strip leading/trailing spaces and split
        if len(values) >= 2:  # Ensure there are at least two values
            lat_point_list.append(values[0])
            lon_point_list.append(values[1])

    new_combine = combine(designation, lon_point_list, lat_point_list)

    # save class
    classes.append(new_combine['class'].to_list())
    # Calculate the area in square meters
    area_m2.append(new_combine.area)
    area_ha.append(new_combine.area / 10000)
    total_overlap_area_m2.append(new_combine.area.sum())
    total_overlap_area_ha.append(new_combine.area.sum() / 10000)

df_spasial['class'] = classes
df_spasial['area_m2'] = area_m2
df_spasial['area_ha'] = area_ha
df_spasial['total_overlap_area_m2'] = total_overlap_area_m2
df_spasial['total_overlap_area_ha'] = total_overlap_area_ha
