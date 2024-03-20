import pandas as pd

def read_csv_from_url(url):
    """Reads CSV file from a given URL and returns the dataframe."""
    return pd.read_csv(url)

def merge_dataframes(df_list):
    """Merges a list of dataframes into a single dataframe."""
    return pd.concat(df_list, ignore_index=True)

def filter_non_zero_delay(df):
    """Filters out rows with Min Delay equal to 0."""
    return df[df['Min Delay'] > 0]

def separate_by_day_hour_delay(merged_df):
    """Separates the merged dataframe into separate dataframes based on day, hour, and delay status."""
    merged_df['Time'] = pd.to_datetime(merged_df['Time'])
    grouped_by_day_hour_delay = merged_df.groupby([merged_df['Day'], merged_df['Time'].dt.hour, merged_df['Min Delay'] == 0])
    
    day_hour_delay_dataframes = {}
    for (day, hour, is_zero_delay), group_df in grouped_by_day_hour_delay:
        delay_category = 'Zero Delay' if is_zero_delay else 'Non-Zero Delay'
        day_hour_delay_dataframes.setdefault(day, {}).setdefault(hour, {}).setdefault(delay_category, group_df)
    
    return day_hour_delay_dataframes

def print_dataframes_info(dataframes_dict):
    """Prints the size of each dataframe."""
    for day, day_hour_dict in dataframes_dict.items():
        for hour, hour_delay_dict in day_hour_dict.items():
            for delay_category, delay_df in hour_delay_dict.items():
                print(f"Size of {day} dataframe at {hour}:00 with {delay_category}: {delay_df.shape[0]} rows x {delay_df.shape[1]} columns")

def main():
    # URLs of the CSV files
    url_2021 = 'https://raw.githubusercontent.com/rjeong1530/TTC-Data-analysis/main/csv/ttc-subway-delay-data-2021.csv'
    url_2022 = 'https://raw.githubusercontent.com/rjeong1530/TTC-Data-analysis/main/csv/ttc-subway-delay-data-2022.csv'
    url_2023 = 'https://raw.githubusercontent.com/rjeong1530/TTC-Data-analysis/main/csv/ttc-subway-delay-data-2023.csv'
    url_2024 = 'https://raw.githubusercontent.com/rjeong1530/TTC-Data-analysis/main/csv/ttc-subway-delay-data-2024.csv'
    # Read CSV files from URLs
    df_2021 = read_csv_from_url(url_2021)
    df_2022 = read_csv_from_url(url_2022)
    df_2023 = read_csv_from_url(url_2023)
    df_2024 = read_csv_from_url(url_2024)
    
    # Merge dataframes
    merged_df = merge_dataframes([df_2023, df_2024])
    
    # Filter rows with Min Delay > 0
    merged_df = filter_non_zero_delay(merged_df)
    
    # Separate dataframes by day, hour, and delay status
    day_hour_delay_dataframes = separate_by_day_hour_delay(merged_df)
    
    # Print size of each dataframe
    print_dataframes_info(day_hour_delay_dataframes)

if __name__ == "__main__":
    main()
