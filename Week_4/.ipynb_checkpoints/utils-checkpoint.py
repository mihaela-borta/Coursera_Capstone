import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from shapely.geometry import Polygon

from datetime import datetime
import time
import os
import multiprocessing as mp

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
        The upper right hand corner of "grid of grids" (the default is the upper right hand [lat, lon] of the USA).

    lower_left: array_like
        The lower left hand corner of "grid of grids"  (the default is the lower left hand [lat, lon] of the USA).

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