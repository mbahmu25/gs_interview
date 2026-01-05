from typing import List, Tuple, Union

from shapely import intersection, wkt
from shapely.geometry import Polygon
import pandas as pd
import geopandas as gpd
import tqdm
longitude = None
latitude = None
level = None
gid = None
address = None

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

print(CODE_ALPHABET_VALUE)
print()
print(CODE_ALPHABET_INDEX)

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
    # return wkt.loads(f"Polygon (({a[0]} {a[1]},{a[0]} {a[3]},{a[2]} {a[3]},{a[2]} {a[1]},{a[0]} {a[1]}))")
def gid_to_lonlat(self, gid: str) -> Tuple[float, float]:
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
            part_x = (lon_ranged[1] - lon_ranged[0]) / self.d[idx]
            part_y = (lat_ranged[1] - lat_ranged[0]) / self.d[idx]
            
            shift_x = part_x * CODE_ALPHABET_VALUE[char][1]
            shift_y = part_y * CODE_ALPHABET_VALUE[char][0]
            
            lon_ranged = (lon_ranged[0] + shift_x, lon_ranged[0] + shift_x + part_x)
            lat_ranged = (lat_ranged[0] + shift_y, lat_ranged[0] + shift_y + part_y)
            
        result = (lon_ranged[0], lat_ranged[0])
        return result

def createLevel14(gid:str,listGrid:list):
    code = ["2", "3", "4", "5", "6","T", "V", "W", "X", "Y","7", "8", "9", "C", "E","F", "G", "H", "J", "L","M", "N", "P", "Q", "R"]
    
    for j in range(len(code)):
        listGrid.append(gid_to_bound(gid+code[j]+CODE_ALPHABET[0][0]))
        listGrid.append(gid_to_bound(gid+code[j]+CODE_ALPHABET[0][1]))
        listGrid.append(gid_to_bound(gid+code[j]+CODE_ALPHABET[1][0]))
        listGrid.append(gid_to_bound(gid+code[j]+CODE_ALPHABET[1][1]))
    # return gid_to_bound()
# print(gid_to_bound("J3N2M827P353"))
# createLevel14("J3N2M827P353")
listGrid = []
# lvl14_grid = gpd.GeoDataFrame(columns=["gid","value"])
gdf = gpd.read_parquet("lvl12_master.parquet")
for i in tqdm.tqdm(range(len(gdf))):
    createLevel14(gdf["gid"].iloc[i],listGrid)
print(len(listGrid))