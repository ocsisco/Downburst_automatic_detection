import pandas as pd
from datetime import timedelta
import os
from dotenv import load_dotenv
from rich.console import Console
import yaml


def get_data_AVAMET():

    # Cargar configuración desde config.yml
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    date = config["date"]

    print(" ")
    console = Console()
    with console.status("[cyan]Downloading AVAMET data...[/cyan]") as status:
        load_dotenv()
        token = os.getenv("AVAMET_PASSWORD")
        all_days = pd.DataFrame()

        if not date:
            now = pd.to_datetime("now")
            one_day_earlier = now - timedelta(days=1)
            today = now.strftime('%Y-%m-%d')
            yesterday = one_day_earlier.strftime('%Y-%m-%d')
            
        else:
            today,yesterday = date,date

        for date in [yesterday,today]:

            df = pd.DataFrame()
            try: data = pd.read_html("https://www.avamet.org/mxo-consulta-diaria.php?id=c%&ini="+date+"&fin="+date+"&token="+token, decimal=",")
            except: df.to_csv("data/dataset_AVAMET.csv", index=False)

            metadata = data[0]
            data = data[1]

            metadata.columns = [col[1] for col in metadata.columns]
            data.rename(columns={"esta":"codi"}, inplace=True)

            df = pd.merge(data, metadata, on="codi", how="inner")
            df = df.reindex()

            df["latitud"] =         df["latitud"]/100000000
            df["longitud"] =        df["longitud"]/100000000
            df["vent vel_mit"] =    df["vent vel_mit"]/10
            df["vent vel_max"] =    df["vent vel_max"]/10
            df["temp mit_mit"] =    df["temp mit_mit"]/10
            df["hrel mit_mit"] =    df["hrel mit_mit"]/10
            df["pres mit_mit"] =    df["pres mit_mit"]/10
            df["prec tot_sum"] =    df["prec tot_sum"]/100
            df["prec tot"] =        df["prec tot"]/100
            df["data ini"] =        pd.to_datetime(df["data ini"])

            print(date+" downloaded")
            all_days = pd.concat([all_days,df], axis=0)

        if not date:
            twelve_hours_ago = now - timedelta(hours=12)
            all_days = all_days[all_days['data ini'] >= twelve_hours_ago]

        all_days = all_days.drop_duplicates()
        all_days.to_csv("data/dataset_AVAMET.csv", index=False)
        print(all_days)


if __name__=="__main__":
    get_data_AVAMET()