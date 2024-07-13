import pandas as pd
from datetime import datetime

def read_csv(file_path):
    """
    Lee el archivo CSV y devuelve un DataFrame.

    Args:
        file_path (str): La ruta del archivo CSV.

    Returns:
        DataFrame: El DataFrame con los datos del CSV.
    """
    return pd.read_csv(file_path)

def convert_olt_names(df):
    """
    Convierte todos los nombres de OLT a mayúsculas y les agrega el prefijo "OLT/".

    Args:
        df (DataFrame): El DataFrame con los datos originales.

    Returns:
        DataFrame: El DataFrame con los nombres de OLT modificados.
    """
    df['OLT_NAME'] = 'OLT/' + df['OLT_NAME'].str.upper()
    return df

def rename_column(df):
    """
    Renombra la columna 'OLT_NAME' a 'DN'.

    Args:
        df (DataFrame): El DataFrame con los datos originales.

    Returns:
        DataFrame: El DataFrame con la columna renombrada.
    """
    df.rename(columns={'OLT_NAME': 'DN'}, inplace=True)
    return df

def resample_and_sum_kpis(df):
    """
    Resamplea la información cada 15 minutos y suma los KPIs especificados.

    Args:
        df (DataFrame): El DataFrame con los datos originales.

    Returns:
        DataFrame: El DataFrame con los datos resampleados y los KPIs sumados.
    """
    kpis_to_sum = ['ESTABLISHED_CALLS', 'FAILED_CALLS', 'NEW_REG', 'EXPIRED_REG', 'FAILED_REG', 'GONE_REG', 'UNAUTHORIZED_REG']
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    df.set_index('TIMESTAMP', inplace=True)
    df_resampled = df.groupby('DN').resample('15min')[kpis_to_sum].sum()
    df_resampled.reset_index(inplace=True)
    return df_resampled

def save_to_csv(df, file_path):
    """
    Guarda el DataFrame transformado en un archivo CSV.

    Args:
        df (DataFrame): El DataFrame con los datos transformados.
        file_path (str): La ruta donde se guardará el archivo CSV.
    """
    df.to_csv(file_path, index=False)

def order_by_columns(df, columns):
    """
    Ordena el DataFrame por las columnas especificadas en orden ascendente.

    Args:
        df (pandas.DataFrame): El DataFrame a ordenar.
        columns (list): Lista de nombres de columnas por las cuales se ordenará el DataFrame.

    Returns:
        pandas.DataFrame: El DataFrame ordenado.
    """
    df.sort_values(by=columns, inplace=True)
    return df