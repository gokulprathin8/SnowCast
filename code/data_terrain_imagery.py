# download the terrain imagery for western U.S. and read values from them
# north west tile: N48W126
# north east tile: N49W099
# south west tile: N26W115
# south east tile: N26W098

# from azureml.opendatasets import SrtmDownloader

import csv
import rioxarray
import xrspatial
import numpy as np
import xarray as xr
import pystac_client
import planetary_computer
from shapely.wkt import loads
from shapely.geometry import Polygon

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

# select western US as our area of interest
bbox = [-125.0, 31.0, -102.0, 49.0]

area_of_interest = {"type": "Polygon", "coordinates": [[
    [bbox[0], bbox[1]],
    [bbox[2], bbox[1]],
    [bbox[2], bbox[3]],
    [bbox[0], bbox[3]],
    [bbox[0], bbox[1]],
]]}

search = catalog.search(collections=["nasadem"], intersects=area_of_interest)
western_us = search.item_collection()

terrian_file = open('/home/chetana/terrian_downloaded_data/terrian_features.csv', 'w+')

for tile in western_us:
    writer = csv.DictWriter(terrian_file, fieldnames=['tile_id', 'lat', 'long', 'northness_30', 'northness_1000',
                                                      'curvature_30', 'curvature_1000', 'slope_30', 'slope_1000',
                                                      'elevation_30', 'elevation_1000'])
    writer.writeheader()
    coordinates = tile.bbox
    polygon = str(Polygon([(coordinates[0], coordinates[1]),
                       (coordinates[2], coordinates[1]),
                       (coordinates[2], coordinates[3]),
                       (coordinates[0], coordinates[3])]))
    polygon = loads(polygon)
    lat_long = list(polygon.exterior.coords)
    da = rioxarray.open_rasterio(tile.assets["elevation"].href, variable="elevation").isel(band=0)
    aspect = xrspatial.aspect(da)
    aspect_30 = aspect.where((da > (30 - 15)) & (da < (30 + 15)), drop=True)
    aspect_1000 = aspect.where((da > (1000 - 15) & (1000 + 15)), drop=True)
    aspect_30_mean = aspect_30.mean()
    aspect_1000_mean = aspect_1000.mean()

    aspect_30 = aspect_30_mean.values  ##
    aspect_1000 = aspect_1000_mean.values  ##

    elevation_30_calc = da.where((aspect_30 > 15) & (aspect_30 < 45))
    elevation_1000_calc = da.where((aspect_1000 > 1000 - 15) & (aspect_1000 < 1000 + 15))
    elevation_30 = elevation_30_calc.mean().values
    elevation_1000 = elevation_1000_calc.mean().values

    curvature_30_calc = xrspatial.curvature(da.where((aspect > 30 - 15) & (aspect < 30 + 15)))
    curvature_30_filter = xr.where((aspect > 30 - 15) & (aspect < 30 + 15), curvature_30_calc, np.nan)

    curvature_1000_calc = xrspatial.curvature(da.where((aspect > 1000 - 15) & (aspect < 1000 + 15)))
    curvature_1000_filter = xr.where((aspect > 1000 - 15) & (aspect < 1000 + 15), curvature_1000_calc, np.nan)

    curvature_30 = curvature_30_filter.mean().values
    curvature_1000 = curvature_1000_filter.mean().values

    slope_30_calc = xrspatial.slope(da, 30)
    slope_1000_calc = xrspatial.slope(da, 1000)

    slope_30 = slope_30_calc.mean().item()
    slope_1000 = slope_1000_calc.mean().item()

    northness_30_calc = xrspatial.aspect(slope_30_calc)
    northness_1000_calc = xrspatial.aspect(slope_1000_calc)

    northness_30 = northness_30_calc.mean().item()
    northness_1000 = northness_1000_calc.mean().item()

    for l in lat_long:
        print(l[0], l[1])
        row = {'tile_id': tile.id, 'lat': l[1], 'long': l[0], 'northness_30': northness_30, 'northness_1000': northness_1000,
               'curvature_30': curvature_30, 'curvature_1000': curvature_1000, 'slope_30': slope_30,
               'slope_1000': slope_1000, 'elevation_30': elevation_30, 'elevation_1000': elevation_1000}
        writer.writerow(row)

