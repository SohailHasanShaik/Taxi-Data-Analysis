import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

# Mapping of vendor_id to vendor names
Vendor_names = {'HVOOO2': 'Juno', 'HV0003': 'Uber', 'HV0004': 'Via', 'HV0005': 'Lyft'}

def generate_average_trip_time_map(input_folder, output_folder):
    # Load GeoJSON file
    geojson_path = "path_to_geojson_file.geojson"
    nyc_geo = gpd.read_file("../supporting_dataset/ploting_graphs/NYC_Taxi_Zones.geojson")

    # Find all CSV files in the specified input folder
    csv_files = [file for file in os.listdir(input_folder) if file.endswith(".csv")]

    if not csv_files:
        print("No CSV files found in the specified input directory.")
        return

    highest_time_zone = None
    highest_time_value = 0.0

    # Load and merge all datasets with trip time information
    combined_data = None
    for csv_file in csv_files:
        trip_time_data = pd.read_csv(os.path.join(input_folder, csv_file))
        trip_time_data['start_zone'] = trip_time_data['start_zone'].astype(str)
        trip_time_data['trip_time'] = trip_time_data['trip_time'] / 60.0  # Convert time to minutes

        # Find the zone with the highest time duration
        max_time_row = trip_time_data.loc[trip_time_data['trip_time'].idxmax()]
        if max_time_row['trip_time'] > highest_time_value:
            highest_time_value = max_time_row['trip_time']
            highest_time_zone = nyc_geo.loc[nyc_geo['location_id'] == max_time_row['start_zone']]['zone'].values[0]

        if combined_data is None:
            combined_data = trip_time_data
        else:
            combined_data = combined_data.append(trip_time_data, ignore_index=True)

    # Merge combined trip time data with GeoJSON based on start_zone (location_id)
    merged_data = nyc_geo.merge(combined_data, how='left', left_on='location_id', right_on='start_zone')

    # Plot the map for the combined average trip time
    fig, ax = plt.subplots(figsize=(10, 10))
    merged_data.plot(ax=ax, column='trip_time', cmap='Oranges', legend=True, legend_kwds={'label': "Average Trip Time (minutes)"})
    ax.set_title('Average Duration of Trips in New York', fontsize=14, fontweight='bold', color='black')
    ax.set_axis_off()

    # Annotate the zone with the highest time duration
    ax.annotate(f'Zone with highest Average  Trip Duration: {highest_time_zone}', xy=(0.5, -0.05), xycoords='axes fraction',
                ha='center', va='center', fontsize=10, fontweight='bold', color='red')

    # Save the figure
    output_path = os.path.join(output_folder, 'output_combined_average_trip_time_map.png')
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py input_directory output_directory")
        sys.exit(1)

    input_directory = sys.argv[1]
    output_directory = sys.argv[2]

    generate_average_trip_time_map(input_directory, output_directory)
