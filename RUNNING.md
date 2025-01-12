# Taxi Data Analysis Project

Welcome to the Taxi Data Analysis Project! This project involves analyzing taxi datasets to perform various tasks such as prediction, mapping, and visualization. Follow the steps below to set up the project and run the analysis.

## Getting Started

### Prerequisites

- Python
- Virtual environment

### Installation

1. **Create and Activate a Virtual Environment:**

   To isolate our project dependencies, we use a virtual environment. Create it using:

   ```bash
   python -m venv <virtual_environment_name>
   ```

   Activate the virtual environment:

   - On Windows: `<virtual_environment_name>\Scripts\activate`
   - On Unix or MacOS: `source <virtual_environment_name>/bin/activate`

2. **Install Dependencies:**

   Install the required Python packages:

   ```bash
   pip install -r requirements.txt
   ```

3. **Download Datasets:**

   Download the necessary taxi datasets:

   ```bash
   python download_taxi_datasets.py
   ```

4. **Organize Dataset:**

   - Move `yellow_taxi_data` and `green_taxi_data` to the `datasets` directory in the `yellow_and_green_cabs` folder.
   - Move `fhvhv_taxi_data` to the `datasets` directory in the `higher_volume` folder.

## Analysis and Output Generation

### Running Analysis Scripts

1. **Perform Analysis:**

   Execute the following scripts to analyze the data. These scripts will save the results in the `outputs` folder.

   To run these files you must go in there dir

   - For continuous variables grouped by month and year:

     ```bash
     python avg_amounts_speed_time_passenger_counts.py <taxi_type>
     ```

   - For average total amount, time difference, and tip amount by rate code ID:

     ```bash
     python avg_time_diff_and_avg_amounts_by_rate_id.py <taxi_type>
     ```

   Replace `<taxi_type>` with either `green` or `yellow` depending on the dataset you want to analyze.

2. **Prediction and Visualization:**

   - To perform training and save results in CSV format:

     ```bash
     python average_total_amount_rfm.py <taxi_type>
     ```

   - To create maps:

     ```bash
     python map_data.py <taxi_type> 2022 2022
     ```

   This command will generate visualizations for the year 2022.
   [2022, 2022] represents the time interval from 1st Janurary 2022 to 31st December 2022

3. **Executing Visualization Scripts:**

   In the `visualizations` directory of `yellow_and_green_cabs`, execute each file as follows:

Go into Visualizations Dir and run the commands

    ```bash
    python <visualization_file>.py <taxi_type>
    ```

    For example:

    ```bash
    python visualization_by_time.py green
    ```

    For `map_visualizations.py`, specify the date range:

    ```bash
    python map_visualizations.py green
    ```

### Higher Volume Data Analysis

For analyzing higher volume data, execute:

```bash
python avg_amounts_distance_time_by_company_and_month_year.py
```
1. **Basic ETL**
```bash
   spark-submit /etl/code/etl_filtering_data.py /dataset/hvhv_taxi_data /filtered_data

   spark-submit /etl/code/etl_vendor.py /filtered_data /vendor_data
   ```
2. **Shared trips:**

   Spark Commands:
```bash

   spark-submit /shared_trips/spark_code/shared_trip_per_company.py /vendor_data /shared_trips/output/

   spark-submit /shared_trips/spark_code/shared_rush_hours.py /vendor_data /shared_trips/output/

   spark-submit /shared_trips/spark_code/shared_trip_per_zone.py /vendor_data /shared_trips/output/
```
   Python commands:
```bash
   python3 /shared_trips/plot_code/plot_company_growth.py /shared_trips/output/shared_trip_count/ /shared_trips/graphs/

   python3 /shared_trips/plot_code/plot_shared_drop_c.py /shared_trips/output/shared_drop_per_zone/ /shared_trips/graphs/

   python3 /shared_trips/plot_code/plot_shared_rush_hours.py /shared_trips/output/shared_rush_hours/ shared_trips/graphs/
```
2. **Tip Analysis:**
   Spark commands:
```
   spark-submit /tip_analysis/spark_code/tipping_trend.py /vendor_data /tip_analysis/output/

   spark-submit/tip_analysis/spark_code/tip_rush_hours.py /vendor_data /tip_analysis/output/

   spark-submit /tip_analysis/spark_code/average_tip_vendor.py /vendor_data /tip_analysis/output/
```
   Python commands:
```
   python3 /tip_analysis/plot_code/plot_tip_trend.py /tip_analysis/output/tips_trend /tip_analysis/graphs/

   python3 /tip_analysis/plot_code/plot_tip_rush_hours.py /tip_analysis/output/tip_rush_hours /tip_analysis/graphs/

   python3 /tip_analysis/plot_code/plot_average_tip_zone.py /tip_analysis/output/tips_per_zone /mnt/e/Data_Trio/hvfh_mja125/tip_analysis/graphs/

   python3 /tip_analysis/plot_code/plot_distribution_overall.py /tip_analysis/output/pickup_per_zone /tip_analysis/graphs/
```


3. **Market Analysis:**
   Spark Code:
```
   spark-submit /market_analysis/spark_code/distribution_zones.py /vendor_data /market_analysis/output/

   spark-submit /market_analysis/spark_code/average_trip_duration.py /vendor_data /market_analysis/output/
```
   Python Code:
```
   python3 /market_analysis/plot_code/plot_combined_trip_duration_zone.py /tip_analysis/output/trip_duration /tip_analysis/graphs/

   python3 /market_analysis/plot_code/plot_distribution_overall.py /market_analysis/output/pickup_per_zone/ /market_analysis/graphs/ /supporting_dataset/zone_name.csv
```