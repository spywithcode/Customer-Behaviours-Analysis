# Customer Shopping Behavior Data Processing Script
# This script loads, explores, cleans, and enhances customer shopping behavior data from a CSV file.
# It performs data preprocessing tasks such as handling missing values, cleaning column names,
# and creating new features for better analysis.

import pandas as pd

# Configuration: File path for the input CSV
CSV_FILE_PATH = 'customer_shopping_behavior.csv'

def load_data(file_path):
    """
    Load the customer shopping behavior data from a CSV file.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    df = pd.read_csv(file_path)
    print("Data loaded successfully.")
    return df

def initial_data_exploration(df):
    """
    Perform initial exploration of the dataset by printing key statistics and information.

    Args:
        df (pd.DataFrame): The DataFrame to explore.
    """
    print("\n=== Initial Data Exploration ===")
    print("First 5 rows of the dataset:")
    print(df.head())

    print("\nDataset information (columns, data types, non-null counts):")
    print(df.info())

    print("\nDescriptive statistics for numerical columns:")
    print(df.describe())

    print("\nDescriptive statistics for all columns (including categorical):")
    print(df.describe(include='all'))

    print("\nCount of null values in each column:")
    print(df.isnull().sum())

def handle_missing_values(df):
    """
    Handle missing values in the dataset.
    Specifically, fill null values in 'Review Rating' with the median rating per category.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    print("\n=== Handling Missing Values ===")
    # Fill nulls in 'Review Rating' with median per category
    df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))
    print("Filled null values in 'Review Rating' with median per category.")

    print("Updated count of null values in each column:")
    print(df.isnull().sum())

    return df

def clean_column_names(df):
    """
    Clean and standardize column names for better usability.
    - Convert to lowercase
    - Replace spaces with underscores
    - Rename specific columns for consistency

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: DataFrame with cleaned column names.
    """
    print("\n=== Cleaning Column Names ===")
    print("Original column names:")
    print(df.columns.tolist())

    # Convert to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')

    # Rename specific column for clarity
    df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})

    print("Cleaned column names:")
    print(df.columns.tolist())

    return df

def create_age_group(df):
    """
    Create a new categorical column 'age_group' based on age quartiles.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: DataFrame with the new 'age_group' column.
    """
    print("\n=== Creating Age Groups ===")
    labels = ['Young Adult', 'Adult', 'Middle Age', 'Senior']
    df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)
    print("Created 'age_group' column using quartiles.")
    print("Sample of age and age_group:")
    print(df[['age', 'age_group']].head(10))

    return df

def map_purchase_frequency(df):
    """
    Map the 'frequency_of_purchases' categorical values to numerical days.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: DataFrame with the new 'purchases_frequency_day' column.
    """
    print("\n=== Mapping Purchase Frequency to Days ===")
    frequency_mapping = {
        'fortnightly': 14,
        'weekly': 7,
        'monthly': 30,
        'quarterly': 90,
        'bi weekly': 14,
        'annually': 365,
        'every 3 months': 90
    }

    # Ensure consistency by converting to lowercase
    df['frequency_of_purchases'] = df['frequency_of_purchases'].str.lower()
    df['purchases_frequency_day'] = df['frequency_of_purchases'].map(frequency_mapping)
    print("Mapped 'frequency_of_purchases' to 'purchases_frequency_day' in days.")
    print("Sample of purchases_frequency_day and frequency_of_purchases:")
    print(df[['purchases_frequency_day', 'frequency_of_purchases']].head(10))

    return df

def remove_promocode(df):
    print("\n=== Check the promo Code is use ===")
    print((df['discount_applied'] == df['promo_code_used']).all())

    print("\n=== Remove Promo code used ===")
    df = df.drop('promo_code_used', axis=1)
    print(df.columns)
    return df

def add_mySQL(df):
    """
    Connect to MySQL database and insert the DataFrame into a table.
    Also, read and print a sample from the table.

    Args:
        df (pd.DataFrame): The DataFrame to insert into MySQL.

    Returns:
        pd.DataFrame: The original DataFrame (unchanged).
    """
    from sqlalchemy import create_engine

    # MySQL connection parameters (remove trailing commas to make them strings)
    username = 'root'
    password = 'root'
    host = 'localhost'
    port = '3306'
    database = 'customer_behaviours'

    # Create engine
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

    # Write DataFrame to MySQL table
    table_name = "customer"
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Data inserted into MySQL table '{table_name}' successfully.")

    # Read and print a sample from the table (correct table name)
    sample_data = pd.read_sql("SELECT * FROM customer LIMIT 5", engine)
    print("Sample data from MySQL table:")
    print(sample_data)

    return df

def main():
    """
    Main function to execute the data processing pipeline.
    """
    # Load data
    df = load_data(CSV_FILE_PATH)

    # Initial exploration
    initial_data_exploration(df)

    # Data cleaning
    df = handle_missing_values(df)
    df = clean_column_names(df)

    # Feature engineering
    df = create_age_group(df)
    df = map_purchase_frequency(df)
    
    df = remove_promocode(df)
    df = add_mySQL(df)

    print("\n=== Data Processing Complete ===")
    print("Final dataset shape:", df.shape)
    print("Final columns:", df.columns.tolist())

    # Optionally, save the processed data (uncomment if needed)
    # df.to_csv('processed_customer_shopping_behavior.csv', index=False)
    # print("Processed data saved to 'processed_customer_shopping_behavior.csv'")

if __name__ == "__main__":
    main()
