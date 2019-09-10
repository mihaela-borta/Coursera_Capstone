import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import matplotlib as mpl
import geopandas as gpd
from shapely.geometry import Point, Polygon, asShape
from shapely.wkt import loads
from scipy.spatial import cKDTree
import utm

from datetime import datetime
import time
import os
import multiprocessing as mp
import json
import ast
from pprint import pprint

import requests

REGION_LON = [12.4846473,12.7043738,12.6975074,12.4606147]
REGION_LAT = [55.6230173,55.6257311,55.7461135,55.7410889]

MAP_CENTER_LON = 55.676098
MAP_CENTER_LAT = 12.568337

#https://www.jpytr.com/post/analysinggeographicdatawithfolium/
def get_geojson_grid(upper_right, lower_left, n=200):
    from shapely.geometry import Polygon
    """Returns a grid of geojson rectangles, and computes the exposure in each section of the grid based on the vessel data.

    Parameters
    ----------
    upper_right: array_like
        The upper right hand corner of "grid of grids" (the default is the upper right hand [lat, lon]).

    lower_left: array_like
        The lower left hand corner of "grid of grids"  (the default is the lower left hand [lat, lon]).

    n: integer
        The number of rows/columns in the (n,n) grid.

    Returns
    -------

    list
        List of "geojson style" dictionary objects   
    """

    all_boxes = []

    lat_steps = np.linspace(lower_left[0], upper_right[0], n+1)
    lon_steps = np.linspace(lower_left[1], upper_right[1], n+1)

    lat_stride = lat_steps[1] - lat_steps[0]
    lon_stride = lon_steps[1] - lon_steps[0]

    for lat in lat_steps[:-1]:
        for lon in lon_steps[:-1]:
            # Define dimensions of box in grid
            upper_left = [lon, lat + lat_stride]
            upper_right = [lon + lon_stride, lat + lat_stride]
            lower_right = [lon + lon_stride, lat]
            lower_left = [lon, lat]

            # Define json coordinates for polygon
            coordinates = [
                upper_left,
                upper_right,
                lower_right,
                lower_left,
                upper_left
            ]
                        
            center = Polygon(coordinates)
            center = [center.centroid.xy[0][0], center.centroid.xy[1][0]]
            geo_json = {"type": "FeatureCollection",
                        "properties":{
                            "lower_left": lower_left,
                            "upper_right": upper_right,
                            "centroid": center,
                        },
                        "features":[]}

            grid_feature = {
                "type":"Feature",
                "geometry":{
                    "type":"Polygon",
                    "coordinates": [coordinates],
                }
            }

            geo_json["features"].append(grid_feature)
            all_boxes.append(geo_json)            

    return all_boxes

def fill_request(poly):
    """Returns cookies, headers and data needed for http request"""
    #Converter: https://curl.trillworks.com/
    cookies = {
        'has_js': '1',
        '_ga': 'GA1.2.539167313.1562080654',
        '_gid': 'GA1.2.279988217.1562080654',
        '_fbp': 'fb.1.1562080653818.506592223',
        'Drupal.Sparenergi.Dataflow.zip_code': '1000',
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://sparenergi.dk/demo/addresses/map',
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
    }

    data = {
    'viewport[x1]': str(poly['properties']['lower_left'][1]),
    'viewport[x2]': str(poly['properties']['upper_right'][1]),
    'viewport[y1]': str(poly['properties']['lower_left'][0]),
    'viewport[y2]': str(poly['properties']['upper_right'][0]),
    'center[x]': str(poly['properties']['centroid'][1]),
    'center[y]': str(poly['properties']['centroid'][0]),
    'class': 'DemoMapAddressesType',
    'js_module': 'map_addresses',
    'js_callback': 'load_data'
    }

    return headers, cookies, data


def make_request(mytuple):
    headers, cookies, data = mytuple
    try:
        response = requests.post('https://sparenergi.dk/js/map_addresses/load_data', \
        headers=headers, cookies=cookies, data=data, timeout=10)

        if response.ok:
            result = {'status':'success', 
                    'insert_tms':datetime.now(),
                    'data': response.json(),}
        else:
            result = {'status':'failed', 
                    'insert_tms':datetime.now(),
                    'data':None, }
    except Exception as e:
        print(str(e))
        result = {'status':'failed', 
            'insert_tms':datetime.now(),
            'data':None, }
        
        
    tmp_file = './data/sparenergi_{}.txt'.format(os.getpid())
    
    with open(tmp_file, 'a+') as f:
        f.write(str(result))
    
    time.sleep(2)
    return result



def to_latlong(east_north):
    east = east_north[0]
    north = east_north[1]
    try:
        lat_long = list(utm.to_latlon(east,north,32,'N'))
        return lat_long
    except Exception as e:
        print('Error converting {} -- {}'.format(east_north, str(e)))
        return None

def transform_geom(row):
    geom = ast.literal_eval(row['geometry'])
    geom['coordinates'] = [to_latlong(point[0:2]) for point in geom['coordinates'][0]]
    return geom


def add_latlong(mytuple):
    """Trasform the geometry to lat, long and write to new files"""
    file_path, new_dir = mytuple
    df = pd.read_csv(file_path, sep=';')
    df['geometry_ll'] = df.apply(lambda x: transform_geom(x), axis=1)
    file_name = file_path.split('/')[-1]
    new_path = os.path.join(new_dir, file_name)
    df = df.drop(columns=['geometry'])
    df.to_csv(new_path, index=False)
    print('Created {}'.format(new_path))
    time.sleep(2)
    return new_path

def extract_columns(mytuple):
    """Extract values from the dictionary stored in one column, create new columns for them and write to new files"""
    file_path, new_dir, old_col, new_columns_l = mytuple
    df = pd.read_csv(file_path)

    df.drop(columns=['Unnamed: 0'], inplace=True)
    df.rename(columns={'geometry_ll':'geometry'}, inplace=True)
    df['geometry'] = df['geometry'].apply(lambda x: ast.literal_eval(x))
    df['geom'] = df['geometry'].apply(lambda x: correct_polygon(x))
    df['poly'] = df['geom'].apply(lambda x: asShape(x))
    df.drop(columns=['type','geometry', 'geom'], inplace=True)
    df['geometry'] = df['poly'].apply(lambda x: x.centroid)
    df.drop(columns=['poly'], inplace=True)
    print('{} Done initial parsing for {}'.format(datetime.now(), file_path))
    for col in new_columns_l:
        try:
            df[col] = df[old_col].apply(lambda x: ast.literal_eval(x)[col])
        except Exception as e:
            print('Error extracting {} in file {} - {}'.format(col, file_path, str(e)))
            return None
    file_name = file_path.split('/')[-1]
    new_path = os.path.join(new_dir, file_name)
    df = df.drop(columns=[old_col])
    df.to_csv(new_path, sep=';', index=False)
    print('{} Created {}'.format(datetime.now(), new_path))
    time.sleep(2)
    return new_path


def correct_polygon(x):
    x['coordinates'].append(x['coordinates'][0])
    x['coordinates'] = [tuple(x['coordinates'][i]) for i in range(len(x['coordinates']))]
    x['ncoordinates'] = list()
    x['ncoordinates'].append(x['coordinates'])
    x['coordinates'] = x['ncoordinates']
    x.pop('ncoordinates')
    return x

def correct_point(x):
    """Reverses the initial (lon, lat) order of the coordinates"""
    corrected = x
    corrected['coordinates'] = [x['coordinates'][1], x['coordinates'][0]]
    return corrected


def ckdnearest(gdA, gdB, bcol):   
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)))
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA,k=1)
    df = pd.DataFrame.from_dict({'distance': dist.astype(float),
                             bcol : gdB.loc[idx, bcol].values })
    return df

def get_most_recent_year(x):
    result = 0
    try:
        result = int(x)
    except Exception as e:
        result = int(str(x).split(' ')[-1])
    return result

def combine_solar_w_energyclass(mytuple):
    '''
    Merges solar potential information w energy class.
    '''
    start = datetime.now()
    print(start)

    solar_file_path, energy_file_path, out_dir_path = mytuple

    # Read the energy class file and load it into a GeoPandas DataFrame
    energy_cls = pd.read_csv(energy_file_path, sep=';')
    energy_cls['geometry'] = energy_cls['geometry'].apply(lambda x: ast.literal_eval(x))
    energy_cls['geom'] = energy_cls['geometry'].apply(lambda x: correct_point(x))
    energy_cls['features'] = energy_cls['features'].apply(lambda x: ast.literal_eval(x))
    energy_cls['geometry'] = energy_cls['geom'].apply(lambda x: asShape(x))
    energy_cls.drop(columns=['initial_index', 'geom'], inplace=True)
    energy_cls.rename(columns={'features':'en_prop'}, inplace=True)
    geo_en = gpd.GeoDataFrame(energy_cls, geometry='geometry')

    # Read the solar potential files and load it into a GeoPandas DataFrame
    print('[{}][{}] ============================'.format(datetime.now(), solar_file_path))
    solar = pd.read_csv(solar_file_path)
    solar.drop(columns=['Unnamed: 0'], inplace=True)
    solar.rename(columns={'geometry_ll':'geometry'}, inplace=True)
    solar['geometry'] = solar['geometry'].apply(lambda x: ast.literal_eval(x))
    solar['geom'] = solar['geometry'].apply(lambda x: correct_polygon(x))
    solar['poly'] = solar['geom'].apply(lambda x: asShape(x))
    solar.drop(columns=['type','geometry', 'geom'], inplace=True)
    solar['geometry'] = solar['poly'].apply(lambda x: x.centroid)
    solar.drop(columns=['poly'], inplace=True)
    solar.rename(columns={'properties':'pv_prop'}, inplace=True)
    geo_sol = gpd.GeoDataFrame(solar, geometry='geometry')

    # For each solar potential entry, get the nearest energy class entry
    ckdnearest_enclass = ckdnearest(geo_sol, geo_en, 'en_prop')

    # Adding the complete details of solar potential 
    solar_merged = geo_sol.join(ckdnearest_enclass, how='left')
    solar_merged.rename(columns={'geometry':'geometry_sol'}, inplace=True)
    print('[{}] Done merging solar and energy class.'.format(datetime.now()))  

    # We might end up with several matches between the solar potential and energy class
    # Select the pair that has the smallest distance between the two Geo Points
    # Add the distance information to the dataframe. 
    # The Pandas merge operation requires unmutable columns. 
    # Since the dictionary type is mutable, we need to convert it to string to satisfy the requirements.        

    solar_merged['en_prop'] = solar_merged['en_prop'].astype(str)
    energy_cls['en_prop'] = energy_cls['en_prop'].astype(str)
    merged = pd.merge(solar_merged, energy_cls, on='en_prop', how='inner')

    print(merged.columns)
    for col in merged.columns:
        if merged[col].dtype == object:
            merged[col] = merged[col].astype(str)
    merged.drop_duplicates(inplace=True)

    print(merged.columns)

    print('Selecting the pair with the minimum distance. \
        Current total rows: {}'.format(merged.shape[0], datetime.now()))
    best_match = merged.groupby(['geometry', 'geometry_sol']).\
        agg({'distance':'min'})
    best_match.reset_index(inplace=True)          
    #best_match.drop(columns=['index'], inplace=True)

    merged = pd.merge(best_match, merged, \
        on=['geometry', 'geometry_sol', 'distance'], how='left')

    print('Done selecting the pair with the minimum distance. \
        Current total rows: {}'.format(merged.shape[0], datetime.now()))
    
    # Refine the dataframe: extract only the most useful info from the dictionary columns
    merged['byg_id'] = merged['pv_prop'].apply(lambda x: ast.literal_eval(x)['byg_id'])
    merged['solgruppe1'] = merged['pv_prop'].apply(lambda x: ast.literal_eval(x)['solgruppe1'])
    merged['solgruppe2'] = merged['pv_prop'].apply(lambda x: ast.literal_eval(x)['solgruppe2'])
    merged['solgruppe3'] = merged['pv_prop'].apply(lambda x: ast.literal_eval(x)['solgruppe3']) 
    merged.drop(columns=['pv_prop'], inplace=True)

    new_columns = ['EnergyLabelClassification', 'StreetName',
                    'HouseNumber', 'ZipCode', 'CityName',
                    'YearOfConstruction', 'HeatSupply']
    for col in new_columns:
        merged[col] = merged['en_prop'].\
            apply(lambda x: ast.literal_eval(x)['properties'][col]['value'])

    merged.drop(columns=['en_prop'], inplace=True)

    merged['LatestYearOfConstruction'] = merged['YearOfConstruction'].\
    apply(lambda x: get_most_recent_year(x))    
    merged['geometry_sol_lat'] = merged['geometry_sol'].\
        apply(lambda x: str(x).split()[1].strip('(')).astype(float)
    merged['geometry_sol_lon'] = merged['geometry_sol'].\
        apply(lambda x: str(x).split()[2].strip(')')).astype(float)

    merged.drop(columns=['YearOfConstruction', 'geometry_sol'], inplace=True)

    print('[{}] Done extracting data from the dictionary columns.'.format(datetime.now()))

    '''
    # Check again for the best match in terms of phisycal proximity:
    gr_min_dist = merged.groupby(['geometry']).agg({'distance':'min'})
    gr_min_dist.reset_index(inplace=True)
    merged = pd.merge(gr_min_dist, merged, on=gr_min_dist.columns.tolist(), how='left')
    '''
    out_filename = 'enclass_{}'.format(solar_file_path.split('/')[-1])
    out_file_path = os.path.join(out_dir_path, out_filename)
    merged.to_csv(out_file_path, sep=';', index=False)

    print('[{}] Done writing to file: {}'.format(datetime.now(), out_file_path)) 
    
    return out_file_path