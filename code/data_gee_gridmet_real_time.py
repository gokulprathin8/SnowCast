from snowcast_utils import *
import traceback
import eeauth as e
from multiprocessing import Pool, cpu_count
from snowcast_utils import test_start_date as start_date, test_end_date as end_date

#exit() # done, uncomment if you want to download new files.

try:
    ee.Initialize(e.creds())
except Exception as e:
    ee.Authenticate() # this must be run in terminal instead of Geoweaver. Geoweaver doesn't support prompt.
    ee.Initialize()

homedir = os.path.expanduser('~')
github_dir = f"{homedir}/Documents/GitHub/SnowCast"
submission_format_file = f"{github_dir}/data/snowcast_provided/submission_format_eval.csv"

submission_format_df = pd.read_csv(submission_format_file, header=0, index_col=0)
all_cell_coords_file = f"{github_dir}/data/snowcast_provided/all_cell_coords_file.csv"
all_cell_coords_pd = pd.read_csv(all_cell_coords_file, header=0, index_col=0)

org_name = 'gridmet'
product_name = 'IDAHO_EPSCOR/GRIDMET'

# start_date = findLastStopDate(f"{github_dir}/data/sim_testing/{org_name}/", "%Y-%m-%d %H:%M:%S")
# end_date = test_end_date

end_date_s = datetime.datetime.strptime('2023-03-14', '%Y-%m-%d')
end_date = end_date_s.strftime('%Y-%m-%d')

var_list = ['tmmn', 'tmmx', 'pr', 'vpd', 'eto', 'rmax', 'rmin', 'vs']


dfolder = f"{homedir}/Documents/GitHub/SnowCast/data/sim_testing/{org_name}/"
if not os.path.exists(dfolder):
  os.makedirs(dfolder)
  
column_list = ['date', 'cell_id', 'latitude', 'longitude']
column_list.extend(var_list)
reduced_column_list = ['date']
reduced_column_list.extend(var_list)

all_cell_df = pd.DataFrame(columns = column_list)


count = 0
'''
for current_cell_id in submission_format_df.index:

  try:
    count += 1

    longitude = all_cell_coords_pd['lon'][current_cell_id]
    latitude = all_cell_coords_pd['lat'][current_cell_id]

    # identify a 500 meter buffer around our Point Of Interest (POI)
    poi = ee.Geometry.Point(longitude, latitude).buffer(1000)
    viirs = ee.ImageCollection(product_name).filterDate(start_date, end_date).filterBounds(poi).select(var_list)

    def poi_mean(img):
      reducer = img.reduceRegion(reducer=ee.Reducer.mean(), geometry=poi, scale=1000)
      img = img.set('date', img.date().format());
      for var in var_list:
        column_name = var
        mean = reducer.get(column_name)
        img = img.set(column_name,mean)
      return img


    poi_reduced_imgs = viirs.map(poi_mean)

    nested_list = poi_reduced_imgs.reduceColumns(ee.Reducer.toList(9), reduced_column_list).values().get(0)
    df = pd.DataFrame(nested_list.getInfo(), columns=reduced_column_list)

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    df['cell_id'] = current_cell_id
    df['latitude'] = latitude
    df['longitude'] = longitude
    
    df_list = [all_cell_df, df]
    all_cell_df = pd.concat(df_list) # merge into big dataframe
    break
  except Exception as e:
    print(traceback.format_exc())
    print("Failed: ", e)
    pass

all_cell_df.to_csv(f"{dfolder}/all_vars_{start_date}_{end_date}.csv")  
print(f"{dfolder}/all_vars_{start_date}_{end_date}.csv")
print('DONE')

'''
# Define the function for processing a single cell
def process_cell(current_cell_id):
    try:
        longitude = all_cell_coords_pd.loc[current_cell_id, 'lon']
        latitude = all_cell_coords_pd.loc[current_cell_id, 'lat']

        # identify a 500 meter buffer around our Point Of Interest (POI)
        poi = ee.Geometry.Point(longitude, latitude).buffer(1000)
        
        # get data for the entire area and filter to the specific cell
        viirs_all = ee.ImageCollection(product_name).filterDate(start_date, end_date).select(var_list)
        viirs_cell = viirs_all.filterBounds(poi)

        def poi_mean(img):
            reducer = img.reduceRegion(reducer=ee.Reducer.mean(), geometry=poi, scale=1000)
            img = img.set('date', img.date().format());
            for var in var_list:
                column_name = var
                mean = reducer.get(column_name)
                img = img.set(column_name, mean)
            return img

        poi_reduced_imgs = viirs_cell.map(poi_mean)

        nested_list = poi_reduced_imgs.reduceColumns(ee.Reducer.toList(9), reduced_column_list).values().get(0)
        df = pd.DataFrame(nested_list.getInfo(), columns=reduced_column_list)

        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

        df['cell_id'] = current_cell_id
        df['latitude'] = latitude
        df['longitude'] = longitude

        return df
    except Exception as e:
        print(traceback.format_exc())
        print("Failed: ", e)
        return None

# Define the main function for processing all cells
def process_all_cells():
    with Pool() as p:
        dfs = p.map(process_cell, submission_format_df.index)
    
    dfs = [df for df in dfs if df is not None]
    all_cell_df = pd.concat(dfs)

    all_cell_df.to_csv(f"{dfolder}/all_vars_{start_date}_{end_date}.csv")
    print(f"{dfolder}/all_vars_{start_date}_{end_date}.csv")
    print('DONE')

# Call the main function
process_all_cells()

