import paramiko
import os
from dotenv import load_dotenv


def send_data():
    # Cargar variables de entorno
    load_dotenv()
    SFTP_HOST = os.getenv("FTP_HOST")
    SFTP_USER = os.getenv("FTP_USER")
    SFTP_PASS = os.getenv("FTP_PASS")

    # Archivos
    archivo_local = "data/downbursts.csv"
    archivo_remoto = "/home/downburs/www/form/data/downbursts.csv"

    try:
        # Establecer conexiÃ³n
        transport = paramiko.Transport((SFTP_HOST, 22))
        transport.connect(username=SFTP_USER, password=SFTP_PASS) 

        sftp = paramiko.SFTPClient.from_transport(transport)

        # Subir el archivo JSON
        sftp.put(archivo_local, archivo_remoto)
        print(f"? Archivo '{archivo_local}' subido correctamente a '{archivo_remoto}'.")

        # Cerrar conexiÃ³n
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"? Error al enviar el archivo: {e}")


if __name__ == "__main__":
    send_data()
