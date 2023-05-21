# feed testing.csv into ML model

testing_pd = read_csv("final_testing_ready.csv")

model = joblib.load("joblib file reload")

final_testing_results = mode.predict(testing_pd)

# match final result values with the original input row's lat/lon
# use GDAL rasterio or just Python RasterIO package to write to file 
# refer to https://rasterio.readthedocs.io/en/stable/quickstart.html#saving-raster-data
convert_result_to_image(final_testing_results, "final_ml_result_swe_map.tif")


