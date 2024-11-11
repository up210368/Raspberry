import os
import time
import subprocess
import dht11
import RPi.GPIO as gpio
import ffmpeg
import requests
from azure.storage.blob import BlobServiceClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

gpio.setmode(gpio.BOARD)
DHT_PIN = 11
KY_015 = dht11.DHT11(pin=DHT_PIN)

SOUND_PIN = 7
gpio.setup(SOUND_PIN, gpio.IN)

# Conectar a la cuenta de almacenamiento
connection_string = ""
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

try: #Comprobar conexion a azure
    containers = blob_service_client.list_containers()
    print("Conexion existosa")
    for container in containers:
        print(container.name)
except Exception as e:
    print(f"Error de conexion: {e}")

# Nombres de los contenedores
photo_container_name = "fotos"
video_container_name = "videos"

# Configuracion de Azure Video Indexer
video_indexer_api_key = ""
video_indexer_account_id = ""
video_indexer_location = "trial"  # Cambia si usas otro recurso de Azure Video Indexer

# Funcion para obtener el token de acceso de Azure Video Indexer
def get_video_indexer_token():
    url = f"https://api.videoindexer.ai/Auth/{video_indexer_location}/Accounts/{video_indexer_account_id}/AccessToken"
    headers = {"Ocp-Apim-Subscription-Key": video_indexer_api_key}
    response = requests.get(url, headers=headers)
    return response.text.strip('"')

# Funcion para enviar el video a Azure Video Indexer
def upload_to_video_indexer(blob_url, video_name):
    access_token = get_video_indexer_token()
    url = f"https://api.videoindexer.ai/{video_indexer_location}/Accounts/{video_indexer_account_id}/Videos"
    params = {
        "accessToken": access_token,
        "name": video_name,
        "videoUrl": blob_url,
        "privacy": "Private"
    }
    response = requests.post(url, params=params)
    if response.status_code == 200:
        print(f"Video '{video_name}' enviado a Azure Video Indexer.")
    else:
        print("Error al enviar el video a Azure Video Indexer:", response.text)

# Carpeta a monitorear
monitored_folder = os.path.join(os.path.dirname(__file__), "media")
if not os.path.exists(monitored_folder):
    os.makedirs(monitored_folder)

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Ejecuta esto solo si es un archivo
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)

            # Determinar el contenedor según la extensión del archivo
            if file_name.endswith(('.jpg', '.jpeg', '.png')):
                container_client = blob_service_client.get_container_client(photo_container_name)
            elif file_name.endswith(('.mp4', '.avi', '.mov')):
                container_client = blob_service_client.get_container_client(video_container_name)
            else:
                print("Tipo de archivo no compatible")
                return

            # Intentar subir y luego eliminar el archivo
            try:
                # Subir archivo a Blob Storage
                with open(file_path, "rb") as data:
                    blob_client = container_client.upload_blob(name=file_name, data=data)
                    print(f"Archivo '{file_name}' subido a '{container_client.container_name}'.")

                # Obtener la URL del blob
                blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_client.container_name}/{file_name}"

                # Enviar el video a Azure Video Indexer si es un archivo de video
                if file_name.endswith(('.mp4', '.avi', '.mov')):
                    upload_to_video_indexer(blob_url, file_name)
                # Eliminar archivo local después de la subida exitosa
                os.remove(file_path)
                print(f"Archivo '{file_name}' eliminado localmente.")

            except Exception as e:
                print(f"Error al subir el archivo: {e}")

def capture_photo():
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    photo_path = os.path.join(monitored_folder, f"photo_{timestamp}.jpg")
    subprocess.run(["libcamera-still", "-o", photo_path])
    print(f"Foto capturada en: {photo_path}")

def capture_video(duration):
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    h264_path = os.path.join(monitored_folder, f"video_{timestamp}.h264")
    video_path = os.path.join(monitored_folder, f"video_{timestamp}.mp4")
    subprocess.run(["libcamera-vid", "-t", str(duration * 1000), "-o", h264_path])
    conversion_command = f"ffmpeg -i {h264_path} -c copy {video_path}"
    subprocess.run(conversion_command, shell=True)
    print(f"Video convertido a mp4: {video_path}")

# Configurar el observador para la carpeta monitoreada
event_handler = FileHandler()
observer = Observer()
observer.schedule(event_handler, monitored_folder, recursive=False)
observer.start()

try:
    while True:
        result = KY_015.read()
        if result.is_valid():
            print(f"Temperatura: {result.temperature}ºC, Humedad: {result.humidity}%")
        else:
            print("Error de lectura")
        
        sonido = gpio.input(SOUND_PIN)
        if sonido == 0:
            print("Silencio")
        elif sonido == 1:
            print("Sonido detectado")
        
        time.sleep(4)
        
        capture_photo()
        time.sleep(15) #cada x seg captura una imagen

        capture_video(25) #video de 5 seg
        time.sleep(30)
except KeyboardInterrupt:
    observer.stop()
finally:
    gpio.cleanup()
    observer.join()