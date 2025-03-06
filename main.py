from get_data import get_data_AVAMET
from detect_downbursts import search_downbursts
from generate_output import generate_json
from time import sleep
from rich import print
import yaml


while True:

    # Cargar configuración desde config.yml
    with open("config.yml", "r") as file:
        config = yaml.safe_load(file)
    
    # Run the functions
    try: print(""), print("[bold green]- Descargando datos de AVAMET[/bold green]"), get_data_AVAMET(config["data"]["date"])
    except: print("[bold red]- No se han podido descargar los datos[/bold red]")

    try: print(""), print("[bold green]- Buscando downbursts en el dataset[/bold green]"), search_downbursts()
    except: print("[bold red]- No se han podido buscar downbursts[/bold red]")

    try: print(""), print("[bold green]- Generando json para api[/bold green]"), generate_json()
    except: print("[bold red]- No se ha podido generar el archivo.json[/bold red]")

    print("")
    print("[bold green]_________ END LOOP __________[/bold green]")
    print("")
    sleep(10)