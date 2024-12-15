from airflow.decorators import task
import pandas as pd
import matplotlib.pyplot as plt

def generate_time_series_report(data: pd.DataFrame, plot_path: str) -> str:
    """
    Generate the plot of a time series report showing total sales per day.

    Parameters:
    ----------
    data : pd.DataFrame
        The input DataFrame with columns `transaction_date` and `total_amount`.
    plot_path : str
        The path where the plot image will be saved.

    Returns:
    -------
    str
        The file paths where the report and plot were saved.
    """
    # Plot the time series data
    plt.figure(figsize=(10, 6))
    plt.plot(data['transaction_date'], data['total_sales'], marker='o', color='b', label='Total Sales')
    plt.title('Total Sales per Day')
    plt.xlabel('Date')
    plt.ylabel('Total Sales Amount')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    
    # Save the plot to the specified file path
    plt.savefig(plot_path)
    plt.close()  # Close the plot to free memory

    return f"Plot saved at: {plot_path}"
