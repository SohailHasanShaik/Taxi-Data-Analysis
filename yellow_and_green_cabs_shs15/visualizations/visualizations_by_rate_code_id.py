import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

taxi_color = sys.argv[1]

dataset_path = f'../outputs/{taxi_color}_taxi/avg_time_diff_and_avg_amounts_by_rate_id/final_result/final_result.csv'
df = pd.read_csv(dataset_path)

output_folder_path = f'Images/{taxi_color}_taxi'
os.makedirs(output_folder_path, exist_ok=True)

##################################################################################
# Average Total Amount by RatecodeID
##################################################################################
# Creating a bar plot
plt.figure(figsize=(10, 6))
sns.barplot(hue='RatecodeID', y='avg_total_amount', data=df, palette = 'Set1')

# Adding titles and labels
plt.title('Average Total Amount by RatecodeID')
plt.xlabel('RatecodeID')
plt.ylabel('Average Total Amount')

plt.savefig(f'{output_folder_path}/average_total_amount_by_rate_code_id.png')

##################################################################################
# Average Time Difference by Rate Code ID
##################################################################################
plt.figure(figsize=(10, 6))
sns.barplot(hue='RatecodeID', y='avg_time_diff', data=df, palette = 'Set1')
plt.title('Average Time Difference by Rate Code ID')
plt.xlabel('Rate Code ID')
plt.ylabel('Average Time Difference')
# Save the plot
plt.savefig(f'{output_folder_path}/average_time_difference_by_rate_code_id.png')


##################################################################################
# Average Tip Amounts by Rate Code ID
##################################################################################
plt.figure(figsize=(10, 6))
sns.set(style="whitegrid")

# Exploding the smallest slices
explode_values = [0.1 if amt < 1 else 0 for amt in df['avg_tip_amount']]

# Calculating total sum for percentages
total = df['avg_tip_amount'].sum()

# Pie chart without internal values and external labels
wedges, texts = plt.pie(df['avg_tip_amount'], explode=explode_values)

plt.title('Percentage of Average Tip Amounts by Rate Code ID')

# Creating a combined legend with RatecodeID and average tips as percentages
legend_labels = [f'ID {id}: {amt/total:.1%}' for id, amt in zip(df['RatecodeID'], df['avg_tip_amount'])]
plt.legend(wedges, legend_labels, title="RatecodeID and Average Tips", loc="center left", bbox_to_anchor=(1, 0.5))

# Adjusting layout to accommodate the legend
plt.subplots_adjust(right=0.75)

# Save the plot
plt.savefig(f'{output_folder_path}/tip_amounts_by_rate_code_id.png')