import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

#Importing the Data
file_path = "C:\\Users\\garre\\OneDrive\\Desktop\\Coding\\Gas Monitors\\Data Raw.xlsx"
sheet_name = ['Monitor 1', 'Monitor 2', 'Monitor 3']
df_dict = pd.read_excel(file_path, sheet_name=sheet_name)

#Define the DFs
df_monitor1 = df_dict['Monitor 1']
df_monitor2 = df_dict['Monitor 2']
df_monitor3 = df_dict['Monitor 3']

#Create Monitor Dataframe
monitor_locations = {
    'Monitor': ['498070562RN', '498070557RN', '498070560RN'],
    'Location': ['325-398', '325-487', '458-487']}
monitor_locations = pd.DataFrame(monitor_locations)

class H2S_Aggregator:
    def __init__(self, df, monitor_name):
        self.df = df.copy()
        self.monitor_name = monitor_name
        self._clean_data()
    
    def _clean_data(self):
        self.df.columns = self.df.columns.str.strip()
        if 'Date' in self.df.columns and 'H2S (ppm)' in self.df.columns:
            self.df['Date'] = pd.to_datetime(self.df['Date'], errors='coerce')
            self.df['H2S (ppm)'] = pd.to_numeric(self.df['H2S (ppm)'], errors='coerce')
            self.df.dropna(subset=['Date', 'H2S (ppm)'], inplace=True)
            self.df.set_index('Date', inplace=True)
        else:
            raise ValueError(f"DataFrame for {self.monitor_name} must contain 'Date' and 'H2S (ppm)' columns")
    
    def get_aggregated_data(self, freq):
        avg = self.df.resample(freq).mean()
        avg.reset_index(inplace=True)
        avg['Date'] = avg['Date'].dt.strftime('%Y-%m-%d')
        avg['Average'] = avg['H2S (ppm)']
        percentile_75 = self.df.resample(freq).quantile(0.75)
        percentile_75.reset_index(inplace=True)
        max = self.df.resample(freq).max()
        max.reset_index(inplace=True)
        avg['Percentile_75th'] = percentile_75['H2S (ppm)']
        avg['Monitor'] = self.monitor_name
        avg['Max'] = max['H2S (ppm)']
        return avg[['Date', 'Monitor', 'Average', 'Percentile_75th', 'Max']]
    
#Process data for all monitors
aggregators = {
    'Monitor 1': H2S_Aggregator(df_monitor1, '1'),
    'Monitor 2': H2S_Aggregator(df_monitor2, '2'),
    'Monitor 3': H2S_Aggregator(df_monitor3, '3')}

daily_list = []
weekly_list = []
monthly_list = []

for name, aggregator in aggregators.items():
    daily_list.append(aggregator.get_aggregated_data('D'))
    weekly_list.append(aggregator.get_aggregated_data('W'))
    monthly_list.append(aggregator.get_aggregated_data('ME'))

# Combine into unified DataFrames
daily_df = pd.concat(daily_list, ignore_index=True)
weekly_df = pd.concat(weekly_list, ignore_index=True)
monthly_df = pd.concat(monthly_list, ignore_index=True)

daily_df['Monitor'] = daily_df['Monitor'].astype(int)
weekly_df['Monitor'] = weekly_df['Monitor'].astype(int)
weekly_df['Monitor'] = weekly_df['Monitor'].astype(int)

#Calculate the Amount of Data in Certain Ranges
def monitor_percentages(df):
    df['H2S (ppm)'] = pd.to_numeric(df['H2S (ppm)'], errors='coerce')
    total = df['H2S (ppm)'].sum()

    safe_values = df[df['H2S (ppm)'] < 4]['H2S (ppm)']
    detectable_values = df[(df['H2S (ppm)'] >= 4) & (df['H2S (ppm)'] < 10)]['H2S (ppm)']
    hazardous_values = df[df['H2S (ppm)'] >= 10]['H2S (ppm)']

    percentage_safe_values = (safe_values.sum() / total) * 100
    percentage_detectable_values = (detectable_values.sum() / total) * 100
    percentage_harzardous_values = (hazardous_values.sum() / total) * 100

    return percentage_safe_values, percentage_detectable_values, percentage_harzardous_values

monitor1_percent = monitor_percentages(df_monitor1)
monitor2_percent = monitor_percentages(df_monitor2)
monitor3_percent = monitor_percentages(df_monitor3)

df_percentages = pd.DataFrame([
    {'Monitor': '1', 'Safe': monitor1_percent[0], 'Detectable': monitor1_percent[1], 'Hazardous': monitor1_percent[2]},
    {'Monitor': '2', 'Safe': monitor2_percent[0], 'Detectable': monitor2_percent[1], 'Hazardous': monitor2_percent[2]},
    {'Monitor': '3', 'Safe': monitor3_percent[0], 'Detectable': monitor3_percent[1], 'Hazardous': monitor3_percent[2]},
])

#Generate descriptive statistics for each monitor
descriptive_stats = []

for name, aggregator in aggregators.items():
    stats = aggregator.df['H2S (ppm)'].describe()
    stats_dict = {'Monitor': name}
    stats_dict.update(stats.to_dict())
    descriptive_stats.append(stats_dict)

# Convert to a DataFrame
combined_stats = pd.DataFrame(descriptive_stats)

combined_stats.rename(columns={
    'Monitor': 'monitor',
    'std': 'stand_dev',
    '25%': 'q1',
    '50%': 'median',
    '75%': 'q3'}, inplace=True)

combined_stats[['count', 'mean', 'stand_dev', 'min', 'q1', 'median', 'q3', 'max']] = combined_stats[['count', 'mean', 'stand_dev', 'min', 'q1', 'median', 'q3', 'max']].astype(int)

print(combined_stats.info())