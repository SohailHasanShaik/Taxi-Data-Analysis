import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
import geopandas as gpd
from shapely import wkt
import sys

taxi_color = sys.argv[1]

dataset_path = f'../outputs/{taxi_color}_taxi/map/final_result/final_result.csv'
df = pd.read_csv(dataset_path)

output_folder_path = f'Images/{taxi_color}_taxi/map'
os.makedirs(output_folder_path, exist_ok=True)

# Convert the 'geometry' column from WKT strings to shapely geometries
df['geometry'] = df['geometry'].apply(wkt.loads)

# Now create the GeoDataFrame
zones_shape = gpd.GeoDataFrame(df, geometry='geometry')

###########################################################################################
# Average Tip Amount
###########################################################################################

# Create a Figure object and an Axes object
fig, ax = plt.subplots(figsize=(12, 8))

# Plot the data
zones_shape.plot(column="avg_tip_amount", cmap="viridis", ax=ax, legend = True)

# Add a legend and colorbar
ax.set_title("Average Tip Amount")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.savefig(f'{output_folder_path}/Average Tip Amount.png'.replace(' ', '_'))

###########################################################################################
# Average Time Difference
###########################################################################################
# Create a Figure object and an Axes object
fig, ax = plt.subplots(figsize=(12, 8))

# Plot the data
zones_shape.plot(column="avg_time_diff", cmap="viridis", ax=ax, legend = True)

# Add a legend and colorbar
ax.set_title("Average Time Difference")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.savefig(f'{output_folder_path}/Average Time Difference.png'.replace(' ', '_'))