import pandas as pd
from functools import reduce
from rich.progress import Progress
import yaml


def get_station_codes(df):
    codes = df["codi"].unique().tolist()
    return codes



def anomaly_increase_or_decrease_values(df, station_code, variable, threshold, time_interval, only_increase):
    """
    Parameters:
        df (pd.DataFrame): DataFrame con los datos.
        station_code (str): Código de la estación a analizar.
        variable (str): Nombre de la columna donde buscar cambios bruscos.
        threshold (float): Umbral de cambio (absoluto).
        time_interval (str): Intervalo de tiempo para threshold (1h, 1D, 30min...).
        only_increase (bool): Si es True, solo detecta incrementos; si es False, detecta también descensos.

    Return:
        pd.DataFrame: Filas donde se detectaron cambios bruscos.
    """

    df_filtered = df[df["codi"] == station_code].copy()
    df_filtered["data ini"] = pd.to_datetime(df_filtered["data ini"])
    df_filtered.set_index('data ini', inplace=True)

    filas_filtradas = []
    for i in range(1, len(df_filtered)):  # Empezamos desde 1 para poder comparar con la fila anterior
        timestamp_actual = df_filtered.index[i]
        intervalo_anterior = timestamp_actual - pd.Timedelta(time_interval)

        # Filtramos las filas en el intervalo anterior
        df_intervalo_anterior = df_filtered[(df_filtered.index > intervalo_anterior) & (df_filtered.index < timestamp_actual)]
        
        # Si no hay datos previos en el intervalo, pasamos a la siguiente iteración
        if df_intervalo_anterior.empty:
            continue
        
        # Calculamos la media del intervalo anterior
        media_intervalo_anterior = df_intervalo_anterior[variable].mean()

        valor_actual = df_filtered.iloc[i][variable]

        # Detectar incrementos anómalos
        if valor_actual > media_intervalo_anterior + threshold:
            # Extraemos un df con un intervalo temporal, para que, cuando tenga que hacer mach con otros registros, 
            # no tenga que ser exactamente en el mismo minuto
            intervalo_inicio = timestamp_actual - pd.Timedelta("60min")
            intervalo_fin = timestamp_actual + pd.Timedelta("30min")
            df_rango = df_filtered[
                (df_filtered.index >= intervalo_inicio) & (df_filtered.index <= intervalo_fin)
            ]
            filas_filtradas.append(df_rango)
        
        # Detectar descensos anómalos si no es un acumulado
        if not only_increase and valor_actual < media_intervalo_anterior - threshold:
            # Extraemos un df con un intervalo temporal, para que, cuando tenga que hacer mach con otros registros, 
            # no tenga que ser exactamente en el mismo minuto
            intervalo_inicio = timestamp_actual - pd.Timedelta("60min")
            intervalo_fin = timestamp_actual + pd.Timedelta("30min")
            df_rango = df_filtered[
                (df_filtered.index >= intervalo_inicio) & (df_filtered.index <= intervalo_fin)
            ]
            filas_filtradas.append(df_rango)

    # Evitar errores si no se encontraron anomalías
    if filas_filtradas:
        df_result = pd.concat(filas_filtradas, ignore_index=False)
    else:
        df_result = pd.DataFrame(columns=df.columns)  # DataFrame vacío con mismas columnas

    df_result.index.name = "data ini"
    df_result = df_result.drop_duplicates()
    return df_result



def min_wind_gust(df, station_code, variable, threshold):

    df_filtered = df[df["codi"] == station_code].copy()
    df_filtered["data ini"] = pd.to_datetime(df_filtered["data ini"])
    df_filtered.set_index('data ini', inplace=True)

    # Extraer las filas donde los valores de 'variable' exceden el umbral
    filtered_df = df_filtered[df_filtered[variable] > threshold]
    filtered_df.index.name = 'data ini'
    df = filtered_df
    return df



def search_downbursts():
    """
    Allá adonde se detecte una anomalía se retorna un intervalo de fechas 
    en donde se halle la anomalia, con una ventana lo suficientemente ancha 
    para que haya un periodo de solape entre intervalos en todas las 
    anomalias, si se encuentran las suficientes anomalias en un periodo de
    tiempo, la funcion downburst devuelve la fecha donde se encuentra la
    racha máxima de viento.

    """
    
    # Cargar configuración desde config.yml
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)

    print(" ")
    df = pd.read_csv("data/dataset_AVAMET.csv")
    df1,df2,df3,df4,df5,df6 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    codes = get_station_codes(df)
    lencodes = len(codes)
    with Progress() as progress:
        task = progress.add_task("[cyan]Buscando downbursts...", total=lencodes)
        for code in codes:

            df01 = min_wind_gust(df,
                                 station_code=code,
                                 variable="vent vel_max",
                                 threshold=70)
            
            df02 = anomaly_increase_or_decrease_values(df,
                                                       station_code=code,
                                                       variable='vent vel_max',
                                                       threshold=config["detection"]["vent_vel_max"]["threshold"],
                                                       time_interval=config["detection"]["vent_vel_max"]["time_interval"],
                                                       only_increase=config["detection"]["vent_vel_max"]["only_increase"])
            
            df03 = anomaly_increase_or_decrease_values(df,
                                                       station_code=code,
                                                       variable='vent vel_mit',
                                                       threshold=config["detection"]["vent_vel_mit"]["threshold"],
                                                       time_interval=config["detection"]["vent_vel_mit"]["time_interval"],
                                                       only_increase=config["detection"]["vent_vel_mit"]["only_increase"])
            
            df04 = anomaly_increase_or_decrease_values(df,
                                                       station_code=code,
                                                       variable='hrel mit_mit',
                                                       threshold=config["detection"]["hrel_mit_mit"]["threshold"],
                                                       time_interval=config["detection"]["hrel_mit_mit"]["time_interval"],
                                                       only_increase=config["detection"]["hrel_mit_mit"]["only_increase"])
            
            df05 = anomaly_increase_or_decrease_values(df,
                                                       station_code=code,
                                                       variable='temp mit_mit',
                                                       threshold=config["detection"]["temp_mit_mit"]["threshold"],
                                                       time_interval=config["detection"]["temp_mit_mit"]["time_interval"],
                                                       only_increase=config["detection"]["temp_mit_mit"]["only_increase"])
            
            df06 = anomaly_increase_or_decrease_values(df,  
                                                       station_code=code,
                                                       variable='prec tot_sum',
                                                       threshold=config["detection"]["prec_tot_sum"]["threshold"],
                                                       time_interval=config["detection"]["prec_tot_sum"]["time_interval"],
                                                       only_increase=config["detection"]["prec_tot_sum"]["only_increase"])

            if not df01.empty:df1 = pd.concat([df1,df01], axis=0)
            if not df02.empty:df2 = pd.concat([df2,df02], axis=0)
            if not df03.empty:df3 = pd.concat([df3,df03], axis=0)
            if not df04.empty:df4 = pd.concat([df4,df04], axis=0)
            if not df05.empty:df5 = pd.concat([df5,df05], axis=0)
            if not df06.empty:df6 = pd.concat([df6,df06], axis=0)
            progress.update(task, advance=1) 

    dfs = [df1, df2, df3, df4]  # Lista de DataFrames
    #print(dfs)
    # Merge para quedarnos con las filas en las que todos los dataframes coincidan
    output_df = pd.DataFrame()
    try: df_common = reduce(lambda left, right: left.merge(right, 
        on=["data ini", "codi", 'vent gra_mit', 'vent vel_mit', 'vent vel_max', 'temp mit_mit',
        'hrel mit_mit', 'pres mit_mit', 'prec tot_sum', 'prec tot', 'latitud',
        'longitud', 'altitud', 'nom descriptiu', 'autoritza?'], how='inner'), dfs)
    except:
        output_df = output_df.drop_duplicates()
        output_df.to_csv("data/downbursts.csv")
        return output_df, print(output_df)
    
    # Eliminamos todos los registros quedandonos con solo aquellos que tienen la racha máxima de cada estación.
    codis = get_station_codes(df_common)
    for codi in codis:
        # Filtramos el DataFrame para obtener solo las filas con el 'codi' específico
        df_filtrado = df_common[df_common['codi'] == codi]
        # Obtenemos la fila con el valor máximo en 'vent vel_max'
        fila_maxima = df_filtrado.loc[df_filtrado['vent vel_max'].idxmax()]
        # Concatenamos la fila seleccionada al DataFrame de salida, asegurándonos de no trasponer las filas
        output_df = pd.concat([output_df, fila_maxima.to_frame().T], axis=0, ignore_index=False)

    output_df = output_df.drop_duplicates()
    output_df.to_csv("data/downbursts.csv")
    return output_df, print(output_df)



if __name__=="__main__":
    search_downbursts()