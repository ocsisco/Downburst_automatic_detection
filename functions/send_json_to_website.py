from ftplib import FTP
import os
from dotenv import load_dotenv



def send_data():

    load_dotenv()
    FTP_HOST = os.getenv("FTP_HOST")
    FTP_USER = os.getenv("FTP_USER")
    FTP_PASS = os.getenv("FTP_PASS")

    # Datos del servidor FTP
    archivo_local = "data/downbursts.json"  # Archivo que quieres subir
    archivo_remoto = "downbursts.json"  # Nombre en el servidor

    # Conectar al servidor FTP
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)

    # Abrir el archivo en modo binario y subirlo
    with open(archivo_local, "rb") as file:
        ftp.storbinary(f"STOR {archivo_remoto}", file)

    # Cerrar la conexión
    ftp.quit()

    print(f"Archivo '{archivo_local}' subido como '{archivo_remoto}'")


if __name__ == "__main__":
    send_data()