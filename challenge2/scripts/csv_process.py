from datetime import datetime
from csv_operations import *

def main():
    """
    Función principal que ejecuta todas las transformaciones y guarda el resultado en un CSV.
    """
    input_file = '../data/original/gpon_challenge.csv'
    df = read_csv(input_file)

    # Ordenar el archivo original solo por 'OLT_NAME' y 'TIMESTAMP'
    df_original_sorted = order_by_columns(df.copy(), ['OLT_NAME', 'TIMESTAMP'])
    save_to_csv(df_original_sorted, '../data/original/gpon_challenge_sorted.csv')

    # Transformaciones
    df = convert_olt_names(df)
    df = rename_column(df)

    # Resamplear y sumar KPIs
    df_resampled = resample_and_sum_kpis(df)
    df_resampled = order_by_columns(df_resampled, ['DN', 'TIMESTAMP'])

    # Guardar el archivo resampleado y ordenado
    output_filename = datetime.now().strftime('GPON_CSV_OUTPUT-%H-%M-%S.csv')
    save_to_csv(df_resampled, f'../data/transformed/{output_filename}')

if __name__ == '__main__':
    main()