from typing import List, Tuple, Union

from shapely import intersection, wkt
from shapely.geometry import Polygon, Point
import pandas as pd
import geopandas as gpd
from math import *
longitude = None
latitude = None
level = None
gid = None
address = None

# COPY OF MODIFIED GEOSQUARE GRID

# Initialize constants
CODE_ALPHABET = [
    ["2", "3", "4", "5", "6"],
    ["7", "8", "9", "C", "E"],
    ["F", "G", "H", "J", "L"],
    ["M", "N", "P", "Q", "R"],
    ["T", "V", "W", "X", "Y"],
]

# Pre-compute derived constants for faster lookups
CODE_ALPHABET_ = {
    5: sum(CODE_ALPHABET, []),
    2: sum([c[:2] for c in CODE_ALPHABET[:2]], []),
    "c2": ["2", "3"],
    "c12": ["V", "X", "N", "M", "F", "R", "P", "W", "H", "G", "Q", "L", "Y", "T", "J"],
}
# print("CODE ak" : CODE_ALPHABET_)
CODE_ALPHABET_VALUE = {
    j: (idx_1, idx_2)
    for idx_1, i in enumerate(CODE_ALPHABET)
    for idx_2, j in enumerate(i)
}

CODE_ALPHABET_INDEX = {
    k: {val: idx for idx, val in enumerate(v)}
    for k, v in CODE_ALPHABET_.items()
}

d = [5, 2, 5, 2, 5, 2, 5, 2, 5, 2, 5, 2, 5, 2, 5]
size_level = {
    10000000: 1, 5000000: 2, 1000000: 3, 500000: 4,
    100000: 5, 50000: 6, 10000: 7, 5000: 8,
    1000: 9, 500: 10, 100: 11, 50: 12,
    10: 13, 5: 14, 1: 15,
}

def getPart(gid: str) -> Tuple[float, float]:
    """
    Get Initial part_x and part_y
    """
    lat_ranged = (-216, 233.157642055036)
    lon_ranged = (-217, 232.157642055036)
    
    for idx, char in enumerate(gid):
        part_x = (lon_ranged[1] - lon_ranged[0]) / d[idx]
        part_y = (lat_ranged[1] - lat_ranged[0]) / d[idx]
        
        shift_x = part_x * CODE_ALPHABET_VALUE[char][1]
        shift_y = part_y * CODE_ALPHABET_VALUE[char][0]
        
        lon_ranged = (lon_ranged[0] + shift_x, lon_ranged[0] + shift_x + part_x)
        lat_ranged = (lat_ranged[0] + shift_y, lat_ranged[0] + shift_y + part_y)
        
    result = (part_x,part_y)
    # return result
    a = result
    return result

def gid_to_bound(gid: str) -> Tuple[float, float, float, float]:
    """
    Converts a grid identifier (gid) to geographical bounds.
    This method translates a geosquare grid identifier string to its corresponding
    geographical bounding box coordinates. The method iteratively processes each character
    in the gid to narrow down the geographical area from the initial range.
    Parameters
    ----------
    gid : str
        The grid identifier string to convert to geographical bounds.
    Returns
    -------
    Tuple[float, float, float, float]
        A tuple representing the bounding box as (min_longitude, min_latitude, max_longitude, max_latitude).
    Examples
    --------
    >>> grid.gid_to_bound("AB12")
    (-216.0, -216.0, -215.9, -215.9)  # Example values
    """
    
        
    lat_ranged = (-216, 233.157642055036)
    lon_ranged = (-217, 232.157642055036)
    
    for idx, char in enumerate(gid):
        part_x = (lon_ranged[1] - lon_ranged[0]) / d[idx]
        part_y = (lat_ranged[1] - lat_ranged[0]) / d[idx]
        
        shift_x = part_x * CODE_ALPHABET_VALUE[char][1]
        shift_y = part_y * CODE_ALPHABET_VALUE[char][0]
        
        lon_ranged = (lon_ranged[0] + shift_x, lon_ranged[0] + shift_x + part_x)
        lat_ranged = (lat_ranged[0] + shift_y, lat_ranged[0] + shift_y + part_y)
        
    result = (lon_ranged[0], lat_ranged[0], lon_ranged[1], lat_ranged[1])
    # return result
    a = result
    return result
    # return wkt.lo
def gid_to_centroid(gid: str) -> Tuple[float, float]:
    """
    Docstring for gid_to_centroid
    
    :param gid: Description
    :type gid: str
    :return: Description
    :rtype: Tuple[float, float]
    Create centroid of grid from input GID
    """
    lat_ranged = (-216, 233.157642055036)
    lon_ranged = (-217, 232.157642055036)
    
    for idx, char in enumerate(gid):
        part_x = (lon_ranged[1] - lon_ranged[0]) / d[idx]
        part_y = (lat_ranged[1] - lat_ranged[0]) / d[idx]
        
        shift_x = part_x * CODE_ALPHABET_VALUE[char][1]
        shift_y = part_y * CODE_ALPHABET_VALUE[char][0]
        
        lon_ranged = (lon_ranged[0] + shift_x, lon_ranged[0] + shift_x + part_x)
        lat_ranged = (lat_ranged[0] + shift_y, lat_ranged[0] + shift_y + part_y)
        
    result = ((lon_ranged[0]+lon_ranged[1])/2, (lat_ranged[0]+lat_ranged[1])/2)
    
    return result
    # return wkt.loads(f"Polygon (({a[0]} {a[1]},{a[0]} {a[3]},{a[2]} {a[3]},{a[2]} {a[1]},{a[0]} {a[1]}))")
def gid_to_lonlat(gid: str) -> Tuple[float, float]:
        """
        Convert a grid ID (GID) to geographic coordinates (longitude, latitude).
        This method decodes a grid ID string into the corresponding geographic coordinates
        by progressively narrowing down coordinate ranges based on each character in the GID.
        Each character in the GID represents a specific position in the hierarchical grid system.
        Args:
            gid (str): The grid ID to convert.
        Returns:
            Tuple[float, float]: A tuple containing (longitude, latitude) coordinates
            corresponding to the lower-left corner of the grid cell.
        Example:
            >>> grid.gid_to_lonlat("AB12")
            (23.45, 67.89)
        """
        
            
        lat_ranged = (-216, 233.157642055036)
        lon_ranged = (-217, 232.157642055036)
        
        for idx, char in enumerate(gid):
            part_x = (lon_ranged[1] - lon_ranged[0]) / d[idx]
            part_y = (lat_ranged[1] - lat_ranged[0]) / d[idx]
            
            shift_x = part_x * CODE_ALPHABET_VALUE[char][1]
            shift_y = part_y * CODE_ALPHABET_VALUE[char][0]
            
            lon_ranged = (lon_ranged[0] + shift_x, lon_ranged[0] + shift_x + part_x)
            lat_ranged = (lat_ranged[0] + shift_y, lat_ranged[0] + shift_y + part_y)
            
        result = (lon_ranged[0], lat_ranged[0])
        return result
def lonlat_to_gid(longitude: float, latitude: float, level: int) -> str:
    """
    Convert geographic coordinates (longitude, latitude) to a geospatial grid identifier (GID).
    This method transforms coordinates into a string identifier representing a grid cell
    at the specified precision level. The grid system divides the world into increasingly
    fine cells as the level increases.
    Parameters
    ----------
    longitude : float
        The longitude coordinate in decimal degrees, must be between -180 and 180.
    latitude : float
        The latitude coordinate in decimal degrees, must be between -90 and 90.
    level : int
        The precision level of the grid cell, must be between 1 and 15.
        Higher levels result in smaller (more precise) grid cells.
    Returns
    -------
    str
        A string identifier representing the grid cell containing the provided coordinates.
        The length of the string equals the specified level.
    Raises
    ------
    AssertionError
        If the input coordinates or level are outside their valid ranges.
    Examples
    --------
    >>> grid.lonlat_to_gid(121.5, 31.2, 5)
    'WXYZP'
    """
    
    
    assert -180 <= longitude <= 180, "Longitude must be between -180 and 180"
    assert -90 <= latitude <= 90, "Latitude must be between -90 and 90"
    assert 1 <= level <= 15, "Level must be between 1 and 15"
    
    lat_ranged = (-216, 233.1576420550)
    lon_ranged = (-217, 232.1576420550)
    gid = ""
    
    for part in d[:level]:
        position_x = int((longitude - lon_ranged[0]) / (lon_ranged[1] - lon_ranged[0]) * part)
        position_y = int((latitude - lat_ranged[0]) / (lat_ranged[1] - lat_ranged[0]) * part)
        
        part_x = (lon_ranged[1] - lon_ranged[0]) / part
        part_y = (lat_ranged[1] - lat_ranged[0]) / part
        
        shift_x = part_x * position_x
        shift_y = part_y * position_y
        
        lon_ranged = (lon_ranged[0] + shift_x, lon_ranged[0] + shift_x + part_x)
        lat_ranged = (lat_ranged[0] + shift_y, lat_ranged[0] + shift_y + part_y)
        
        gid += CODE_ALPHABET[position_y][position_x]
        
    return gid

def gid_neighbor(lonlat:tuple,part):
    """
    Docstring for gid_neighbor
    
    :param lonlat: Description
    :type lonlat: tuple
    :param part: Description
    Searching neighbor GID
    Up, left, bottom, right, upper-left, upper-right, bottom-left, bottom-right
    by backsearch current lonlat  -+ 50 meter 
    """
    lon = lonlat[0]
    lat = lonlat[1]
    ul = lonlat_to_gid(lon-part[0],lat+part[0],12)
    u = lonlat_to_gid(lon,lat+part[0],12)
    ur = lonlat_to_gid(lon+part[0],lat+part[0],12)
    r = lonlat_to_gid(lon+part[0],lat,12)
    br = lonlat_to_gid(lon+part[0],lat-part[0],12)
    b = lonlat_to_gid(lon,lat-part[0],12)
    bl = lonlat_to_gid(lon-part[0],lat-part[0],12)
    l = lonlat_to_gid(lon-part[0],lat,12)
    
    return [ul,u,ur,r,br,b,bl,l]


# Read Paruet Data
def IDW(lonlat,neighbor):
    """
    Docstring for IDW
    
    :param lonlat: Description
    :param neighbor: Description
    Calculate interpolation using IDW method
    """
    a = sum([weight(lonlat,tuple(i[1]))*float(i[0].iloc[0]) for i in (neighbor)])
    b = sum([weight(lonlat,tuple(i[1])) for i in neighbor])
    return a/b

def weight(p,p1):
    """
    Docstring for weight
    
    :param p: Description
    :param p1: Description
    Calculate weight for IDW calculation
    """
    return 1/dist(p,p1)**2

def get_value_from_gid(gid):
    """
    Docstring for get_value_from_gid
    
    :param gid: Description
    Get interpolated value from GID
    """
    gdf = gpd.read_parquet("gsq_material_dtengineer.parquet")

    # Fill null value into 0
    gdf['value'] = gdf['value'].fillna(0)

    # VALIDATE GID INPUT
    if len(gid)==14:
        gid_level_12 = gid[:-2]
    elif len(gid)==12:
        try:
            return float(gdf[gdf['gid']==gid]["value"])
        except:
            del gdf
            return "GID out of bound"
    else:
        return "Please input GID level 14 Or Level 12"
    # GET GID LEVEL 14 INPUT CENTROID
    lonlat = gid_to_centroid(gid)
    
    
    is_contained = gdf[gdf.contains(Point(lonlat[0],lonlat[1]))]
    if len(is_contained)==0:
        del gdf
        raise Exception()
    # Search Grid Part interval 
    part = getPart(gid_level_12)
    
    # Search GID Neighbor in 3x3 50 meter 
    # Input : GID Level 12 from GID Level 14
    # grid
    # 1 2 3
    # 8 c 4 
    # 7 6 5 
    neighbor = gid_neighbor(gid_to_lonlat(gid_level_12),part)

    # Map value from coresponding neighbor and check is the neighbor available
    values = []
    for i in neighbor:
        try:
            value = gdf[gdf['gid']==i]['value']
            if len(value)>0:
                values.append([value,gid_to_centroid(i)])
            else:
                pass
        except:
            pass
    # Add current Level 12 Grid Value
    values.append([gdf[gdf['gid']==gid_level_12]['value'],gid_to_centroid(gid_level_12)])

    # clear gdf memory
    del gdf
    # print(lonlat,value)
    return IDW(lonlat,values)


def get_value_from_lonlat(lonlat):
    """
    Docstring for get_value_from_lonlat
    
    :param lonlat: Description
    Get interpolated value from longitude and latitude value
    """
    
    return get_value_from_gid(lonlat_to_gid(lonlat[0],lonlat[1],14))

# No generated AI used in this work, 100% human 
# ~Genta