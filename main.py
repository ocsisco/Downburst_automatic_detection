from functions.get_data import get_data_AVAMET
from functions.detect_downbursts import search_downbursts
from functions.generate_output import generate_json
from functions.send_json_to_website import send_data
from time import sleep
from rich import print
import yaml


while True:


    # Run the functions
    try: print(""), print("[bold green]- Descargando datos de AVAMET[/bold green]"), get_data_AVAMET()
    except: print("[bold red]- No se han podido descargar los datos[/bold red]")

    try: print(""), print("[bold green]- Buscando downbursts en el dataset[/bold green]"), search_downbursts()
    except: print("[bold red]- No se han podido buscar downbursts[/bold red]")

    try: print(""), print("[bold green]- Generando json para api[/bold green]"), generate_json()
    except: print("[bold red]- No se ha podido generar el archivo.json[/bold red]")

    try: print(""), print("[bold green]- Enviando json por ftp al servidor web[/bold green]"), send_data()
    except: print("[bold red]- No se ha podido enviar el archivo.json[/bold red]")

    print("")
    print("[bold green]_________ END LOOP __________[/bold green]")
    print("")

    # Cargar configuración desde config.yml, si el archivo no existe porque esta siendo modificado, esperar 10 segundos
    try:
        with open("config.yml", "r") as file:
            config = yaml.safe_load(file)
        sleep(config["sleep_time"])
    except: print("[bold red]- No se ha podido leer el archivo de configuración[/bold red]"), sleep(10)
