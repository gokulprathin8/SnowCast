# integrate all the data into a ML-ready CSV

def merge_everything_into_csv():
  """
  read all the downloaded and generated geotiffs for every location and write their values into csv
  """
  modis_tif_file = ""
  gridmet_nc_file = ""
  terrain_tif_file = ""
  
  # read them
  for pixel in modis_tifs:
    lat = pixel.lat
    lon = pixel.lon
    
    modis_val = modis_tifs.getval(lat, lon)
    grid_met_val = get_gridmet(lat, lon)
    terrain_vals = get_terrain(lat, lon)
    
    pd.to_csv([modis_val, gridmet_val, terrain_val], "final_testing_ready.csv")
    
merge_everything_into_csv()
