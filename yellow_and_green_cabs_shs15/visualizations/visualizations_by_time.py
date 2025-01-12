# Import the dataset
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
import sys

# Get the taxi color
taxi_color = sys.argv[1]

# Read the datasets
df = pd.read_csv(f'../outputs/{taxi_color}_taxi/avg_amounts_speed_time_passenger_counts/final_result/final_result.csv')
train_df = pd.read_csv(f'../outputs/{taxi_color}_taxi/average_total_amount_rfm/average_total_amount_prediction_for_train_set.csv')
test_df = pd.read_csv(f'../outputs/{taxi_color}_taxi/average_total_amount_rfm/average_total_amount_prediction_for_test.csv')

# Folder to save the result
output_folder_path = f'Images/{taxi_color}_taxi'
os.makedirs(output_folder_path, exist_ok=True)

# convert the month_year to right format
def convert_to_datetime(data_frame):
    # Convert 'month_year' to datetime
    # Sort the DataFrame by 'month_year'
    
    data_frame = data_frame.copy()
    data_frame['month_year'] = pd.to_datetime(data_frame['month_year'], format='%m-%Y')
    
    data_frame.sort_values('month_year', inplace=True)
    return data_frame

df = convert_to_datetime(df)
train_df = convert_to_datetime(train_df)
test_df = convert_to_datetime(test_df)


####################################################################################
# Correlation between Continuous Variables
####################################################################################
# Calculate the correlation matrix
corr_data = df.drop(['month_year'], axis = 1).corr()

# Set up the matplotlib figure
plt.figure(figsize=(15, 7))

# Draw the heatmap
sns.heatmap(corr_data, 
            xticklabels=corr_data.columns, 
            yticklabels=corr_data.columns,
            cmap='coolwarm', 
            annot=True, 
            linewidths=.5)
# Rotate the x-axis labels
# plt.xticks(rotation=45)
plt.title('Correlation between Continuous Variables')

plt.savefig(f'{output_folder_path}/Correlation Between Continuous Variables.png'.replace(' ', '_'))


####################################################################################
# Average Time Difference over Time
####################################################################################
plt.figure(figsize=(15, 7))

# Ensure the 'date' column is in datetime format for proper plotting
df['month_year'] = pd.to_datetime(df['month_year'])

# Sort the DataFrame by 'date'
df.sort_values('month_year', inplace=True)

# Plotting avg_time_diff and avg_speed
plt.plot(df['month_year'], df['avg_time_diff'], label='Average Time Difference', color='blue')

# Customizing the date format on x-axis
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y')) # Example format

# Adding labels and title
plt.xlabel('Time')
plt.ylabel('Average Time Difference')
plt.title('Average Time Difference over Time')
plt.savefig(f'{output_folder_path}/Average Time Difference Over Time.png'.replace(' ', '_'))


####################################################################################
# Average Speed Over Time
####################################################################################
plt.figure(figsize=(15, 7))

# Plotting avg_time_diff and avg_speed
plt.plot(df['month_year'], df['avg_speed'], color='blue')

# Customizing the date format on x-axis
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))

# Adding labels and title
plt.xlabel('Time')
plt.ylabel('Average Speed')
plt.title('Average Speed Over Time')

plt.savefig(f'{output_folder_path}/Average_Speed_Over_Time.png')


####################################################################################
# Distribution of Average Tip Amount
####################################################################################
# KDE Plot for avg_tip_amount
plt.figure(figsize=(15, 7))
sns.kdeplot(df['avg_tip_amount'], fill=True)
plt.title('Distribution of Average Tip Amount')
plt.xlabel('Average Tip Amount')
plt.ylabel('Density')
plt.savefig(f'{output_folder_path}/Distribution of Average Tip Amount.png'.replace(' ', '_'))


####################################################################################
# Time Series of Average Tip Amount
####################################################################################
plt.figure(figsize=(15, 7))
plt.plot(df['month_year'], df['avg_tip_amount'], marker='o', linestyle='-')
# plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=12)) # Adjust the interval as needed
plt.title('Time Series of Average Tip Amount')
plt.xlabel('Time')
plt.ylabel('Average Tip Amount')
plt.savefig(f'{output_folder_path}/Time Series of Average Tip Amount.png'.replace(' ', '_'))


####################################################################################
# Distribution of Average Amounts
####################################################################################
# KDE Plot for avg_total_amount and avg_fare_amount
plt.figure(figsize=(15, 7))
sns.kdeplot(df['avg_total_amount'], fill=True, label='Average Total Amount')
sns.kdeplot(df['avg_fare_amount'], fill=True, label='Average Fare Amount')

plt.title('Distribution of Average Amounts')
plt.xlabel('Amount')
plt.ylabel('Density')
plt.legend()
plt.savefig(f'{output_folder_path}/Distribution of Average Amounts.png'.replace(' ', '_'))

####################################################################################
# Time Series of Average Total Amount
####################################################################################
plt.figure(figsize=(15, 7))
plt.plot(df['month_year'], df['avg_total_amount'], marker='o', linestyle='-')
plt.title('Time Series of Average Total Amount')
plt.xlabel('Time')
plt.ylabel('Average Total Amount')
plt.grid(True)
plt.savefig(f'{output_folder_path}/Time Series of Average Total Amount.png'.replace(' ', '_'))


####################################################################################
# Distribution of Average Total Amount
####################################################################################
# KDE Plot for avg_tip_amount
plt.figure(figsize=(15, 7))
sns.kdeplot(df['avg_fare_amount'], fill=True)
plt.title('Distribution of Average Total Amount')
plt.xlabel('Average Total Amount')
plt.ylabel('Density')
plt.savefig(f'{output_folder_path}/Distribution of Average Total Amount.png'.replace(' ', '_'))\


####################################################################################
# Time Series of Average Total Amount and Average Fare Amount
####################################################################################
plt.figure(figsize=(15, 7))
plt.plot(df['month_year'], df['avg_fare_amount'], marker='o', label='Average Fare Amount')
plt.plot(df['month_year'], df['avg_total_amount'], marker='o', label='Average Total Amount')

# Adding lines for the year 2020 and 2022 to represent the COVID period
plt.axvline(pd.Timestamp('1-2020'), color='red', linestyle='--', lw=2, label='Start of COVID Period (2020)')
plt.axvline(pd.Timestamp('12-2022'), color='green', linestyle='--', lw=2, label='End of COVID Period (2022)')

plt.title('Time Series of Average Total Amount and Average Fare Amount')
plt.xlabel('Time')
plt.ylabel('Average Amount')
plt.legend()
plt.savefig(f'{output_folder_path}/Time Series of Average Total Amount and Average Fare Amount.png'.replace(' ', '_'))


####################################################################################
# Time Series of Actual and Predicted Values - Train Set
####################################################################################
# Convert 'month_year' to datetime
train_df['month_year'] = pd.to_datetime(train_df['month_year'], format='%m-%Y')

# Sort the DataFrame by 'month_year'
train_df.sort_values('month_year', inplace=True)

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(train_df['month_year'], train_df['avg_total_amount'], label='Actual')
plt.plot(train_df['month_year'], train_df['prediction'], label='Predicted', color='red')
plt.gcf().autofmt_xdate() 
plt.title('Time Series of Actual and Predicted Values - Train Set')
plt.xlabel('Time')
plt.ylabel('Total Amount')
plt.legend()
plt.tight_layout()
plt.savefig(f'{output_folder_path}/Time Series of Average Total Amount Train Set.png'.replace(' ', '_'))


####################################################################################
# Time Series of Actual and Predicted Values - Test Set
####################################################################################
# Convert 'month_year' to datetime
test_df['month_year'] = pd.to_datetime(test_df['month_year'], format='%m-%Y')

# Sort the DataFrame by 'month_year'
test_df.sort_values('month_year', inplace=True)

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(test_df['month_year'], test_df['avg_total_amount'], label='Actual')
plt.plot(test_df['month_year'], test_df['prediction'], label='Predicted', color='red')
plt.gcf().autofmt_xdate() 
plt.title('Time Series of Actual and Predicted Values - Test Set')
plt.xlabel('Time')
plt.ylabel('Total Amount')
plt.legend()
plt.tight_layout()
plt.savefig(f'{output_folder_path}/Time Series of Actual and Predicted Values Test Set.png'.replace(' ', '_'))
####################################################################################
####################################################################################
