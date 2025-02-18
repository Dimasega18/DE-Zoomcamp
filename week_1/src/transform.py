import pandas as pd

def fill_null(df: pd.DataFrame, cols: list, method: str = 'mode', custom_value=None) -> pd.DataFrame:
    """
    Fills missing values in specified columns using a chosen method.
    Parameters:
    cols (list): List of columns to process.
    method (str): Method to fill null values ('mode', 'median', 'mean', or 'custom').
    custom_value (optional): Custom value for filling nulls if method='custom'.
    Returns:
    pd.DataFrame: DataFrame with missing values filled.
    """

    if cols is None:
        raise ValueError("You must specify at least one column in 'cols'.")
    if not all(col in df.columns for col in cols):
        raise ValueError("One or more specified columns are not present in the DataFrame.")
    if method not in ['mode', 'median', 'mean', 'custom']:
        raise ValueError("The 'method' parameter must be 'mode', 'median', 'mean', or 'custom'.")
    if method == 'custom' and custom_value is None:
        raise ValueError("If method='custom', you must provide a 'custom_value'.")
    
    for col in cols:
        if df[col].isnull().sum() > 0:
            if method == 'mode':
                df[col] = df[col].fillna(df[col].mode()[0])
            elif method == 'median':
                df[col] = df[col].fillna(df[col].median())
            elif method == 'mean':
                df[col].fillna(df[col].mean())
            else:
                df[col] = df[col].fillna(custom_value)

    return df

def remove_outliers(df: pd.DataFrame, threshold: float = 3, cols: list = None) -> pd.DataFrame:
    """
    Detects and removes outliers using the Z-score method.
    Parameters:
    threshold (float): Z-score threshold for detecting outliers (default: 3).
    cols (list, optional): List of numeric columns to check. If None, all numeric columns are used.
    Returns:
    pd.DataFrame: DataFrame with outliers removed.
    """

    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

    if cols is None:
        cols = numeric_cols
    if not all(col in numeric_cols for col in cols):
        raise ValueError("One or more specified columns are not valid numeric columns.")
    
    numeric_df = df[cols].copy()
    mean_vals = numeric_df.mean()
    
    std_vals = numeric_df.std()
    std_vals[std_vals == 0] = 1
    
    z_scores = (numeric_df - mean_vals) / std_vals
    outlier_mask = (abs(z_scores) > threshold).any(axis=1)
    
    df = df[~outlier_mask]
    
    return df