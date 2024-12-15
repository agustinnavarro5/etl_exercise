
from typing import Union
import pandas as pd
from scipy.stats import zscore

def remove_outliers_zscore(df: pd.DataFrame, column: str, threshold: Union[int, float] = 3) -> pd.DataFrame:
    """
    Remove outliers from a specified column in a DataFrame using the Z-Score method.

    The Z-Score method identifies outliers as values whose Z-Score exceeds a specified threshold. 
    The Z-Score is calculated as:
        Z = (X - mean) / std
    Values with |Z| > threshold are considered outliers.

    Parameters:
    ----------
    df : pandas.DataFrame
        The input DataFrame containing the data.
    column : str
        The name of the column from which to detect and remove outliers.
    threshold : int or float, optional
        The Z-Score threshold for defining outliers (default is 3).

    Returns:
    -------
    pandas.DataFrame
        A DataFrame with outliers removed for the specified column.
    """
    z_scores = zscore(df[column])
    return df[abs(z_scores) <= threshold]


def top_categories(data: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """
    Identifies the top N item categories based on total sales amount.
    The function calculates total sales for each category by multiplying
    `item_price` by `quantity`, sorts the categories by total sales in descending 
    order, and returns the top N categories with the highest total sales.

    Parameters:
    ----------
    data : pandas.DataFrame
        The input DataFrame containing transaction data. It must include the following columns:
        - 'item_category' (str): The category of the item.
        - 'item_price' (float): The price of the item.
        - 'quantity' (int): The number of items purchased.
    top_n : int, optional
        The number of top categories to return (default is 3).

    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing the top N item categories based on total sales, with the following columns:
        - 'item_category' (str): The category of the item.
        - 'total_amount' (float): The total sales amount for the category.
    """
    category_sales = data.groupby('item_category')['total_amount'].sum().reset_index()
    top_categories = category_sales.sort_values(by='total_amount', ascending=False).head(top_n)
    
    return top_categories

import numpy as np

def customer_segmentation(data: pd.DataFrame) -> pd.DataFrame:
    """
    Segments customers into three groups based on their total spending:
    low_spenders, medium_spenders, and high_spenders. The segmentation is
    performed using the quantiles of total spending, with users below the 33rd 
    percentile as low spenders, between the 33rd and 66th percentile as medium spenders, 
    and above the 66th percentile as high spenders.

    Parameters:
    ----------
    data : pandas.DataFrame
        The input DataFrame containing transaction data. It must include the following columns:
        - 'user_id' (int or str): The unique identifier for each user.
        - 'total_amount' (float): The total amount spent by each user. This should be calculated beforehand.

    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing the user_id, total_amount, and the corresponding 'segment' (low_spenders, 
        medium_spenders, or high_spenders) for each user. The returned DataFrame will have the following columns:
        - 'user_id' (int or str): The unique identifier for each user.
        - 'total_amount' (float): The total amount spent by the user.
        - 'segment' (str): The segment the user belongs to ('low_spenders', 'medium_spenders', 'high_spenders').
    
    Notes:
    ------
    - The thresholds for segmentation are based on the quantiles of total spending:
        - Low spenders: Users with spending below the 33rd percentile.
        - Medium spenders: Users with spending between the 33rd and 66th percentiles.
        - High spenders: Users with spending above the 66th percentile.
    """
    # Calculate total spending per user
    user_spending = data.groupby('user_id')['total_amount'].sum().reset_index()
    
    # Define thresholds based on total spending
    low_threshold = user_spending['total_amount'].quantile(0.33)
    high_threshold = user_spending['total_amount'].quantile(0.66)
    
    # Segment users based on their total spending
    conditions = [
        (user_spending['total_amount'] <= low_threshold),  # low_spenders
        (user_spending['total_amount'] > low_threshold) & (user_spending['total_amount'] <= high_threshold),  # medium_spenders
        (user_spending['total_amount'] > high_threshold)  # high_spenders
    ]
    choices = ['low_spenders', 'medium_spenders', 'high_spenders']
    
    user_spending['segment'] = np.select(conditions, choices, default='low_spenders')

    return user_spending

import pandas as pd

import pandas as pd

def cohort_retention_analysis(data: pd.DataFrame, time_period: str = 'M') -> pd.DataFrame:
    """
    Analyzes user retention by cohorts. Users are grouped into cohorts based on the
    month (or any other time period) of their first purchase, and the function calculates
    the percentage of users who return to make purchases in subsequent months (or time periods).

    Parameters:
    ----------
    data : pandas.DataFrame
        The input DataFrame containing transaction data. It must include the following columns:
        - 'user_id' (int or str): The unique identifier for each user.
        - 'transaction_date' (str or datetime): The date of the transaction.

    time_period : str, optional
        The time period to evaluate cohorts ('D' for daily, 'W' for weekly, 'M' for monthly, etc.).
        Default is 'M' (monthly).

    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing cohort retention analysis with the following columns:
        - 'cohort': The cohort of users based on their first purchase month.
        - 'period': The subsequent time period after the first purchase.
        - 'cohort_size': The number of users in the cohort.
        - 'retained_users': The number of users from the cohort who made a purchase in the period.
        - 'retention_rate': The percentage of users from the cohort who returned in the period.

    Notes:
    ------
    - Cohorts are defined by the month (or other time period) of the user's first purchase.
    - The retention rate is calculated as:
        - Retention Rate = (retained_users / cohort_size) * 100
    """
    # Ensure transaction_date is in datetime format
    data['transaction_date'] = pd.to_datetime(data['transaction_date'])

    # Create a 'cohort' column by finding the first transaction period for each user
    data['cohort'] = data.groupby('user_id')['transaction_date'].transform('min').dt.to_period(time_period)

    # Create a 'period' column to represent the period since the user's first transaction
    data['period'] = (data['transaction_date'].dt.to_period(time_period) - data['cohort']).apply(lambda x: x.n)
    
    # Count the number of users in each cohort and period
    cohort_data = data.groupby(['cohort', 'period', 'user_id']).size().reset_index(name='purchase_count')

    # Get cohort size (number of unique users in each cohort)
    cohort_size = cohort_data.groupby('cohort')['user_id'].nunique().reset_index(name='cohort_size')

    # Get the number of retained users in each period for each cohort
    retained_users = cohort_data.groupby(['cohort', 'period'])['user_id'].nunique().reset_index(name='retained_users')

    # Merge cohort size with retained users
    retention = pd.merge(retained_users, cohort_size, on='cohort')

    # Calculate retention rate
    retention['retention_rate'] = (retention['retained_users'] / retention['cohort_size']) * 100

    return retention

