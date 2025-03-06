import pandas as pd
import urllib.parse
import json
from rich.console import Console



def generate_json():
    df = pd.read_csv("data/downbursts.csv")
    df.rename(columns={"Unnamed: 0": "date"}, inplace=True)

    report_url = "https://downburst.csic.es/en/report-a-downburst/?"

    for index,row in df.iterrows():
        endpoint = ("date="+urllib.parse.quote(row["date"])+
                    "&code="+row["codi"]+
                    "&vdir="+str(row["vent gra_mit"])+
                    "&vavg="+str(row["vent vel_mit"])+
                    "&vmax="+str(row["vent vel_max"])+
                    "&tavg="+str(row["temp mit_mit"])+
                    "&hrel="+str(row["hrel mit_mit"])+
                    "&pres="+str(row["pres mit_mit"])+
                    "&precsum="+str(row["prec tot_sum"])+
                    "&prectot="+str(row["prec tot"])+
                    "&lat="+str(row["latitud"])+
                    "&lon="+str(row["longitud"])+
                    "&alt="+str(row["altitud"])+
                    "&desc="+urllib.parse.quote(row["nom descriptiu"]))
        nueva_fila = {"url": report_url+endpoint}
        df.loc[index, 'url'] = nueva_fila['url']  
    # Suponiendo que df ya contiene los datos
    df.to_json("data/downbursts.json", orient="records", indent=4, force_ascii=False)
        # Abre y lee el archivo JSON
    with open('data/downbursts.json', 'r') as file:
        data = json.load(file)
    # Crea un objeto Console para imprimir en la consola
    console = Console()
    # Convertir el JSON a una cadena con indentación
    json_str = json.dumps(data, indent=4)

    # Imprimir las claves y los valores con colores diferentes
    def print_json_colored(data):
        if isinstance(data, dict):
            for key, value in data.items():
                # Imprimir la clave en un color y la valor en otro
                console.print(f"[bold cyan]{key}[/bold cyan]: [green]{value}[/green]")
        elif isinstance(data, list):
            for item in data:
                print_json_colored(item)
        else:
            console.print(f"[green]{data}[/green]")

    print_json_colored(data)



if __name__=="__main__":
    generate_json()

    