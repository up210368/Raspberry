import os
import time
import subprocess
import adafruit_dht
import RPi.GPIO as gpio
import ffmpeg
import requests
from azure.storage.blob import BlobServiceClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Inicializar el sensor DHT11 y el pin de sonido
DHT_PIN = 4
KY_015 = adafruit_dht.DHT11(DHT_PIN)
SOUND_PIN = 17
gpio.setmode(gpio.BCM)
gpio.setup(SOUND_PIN, gpio.IN)

# Conectar a Azure Blob Storage usando una variable de entorno para mayor seguridad
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

try:
    containers = blob_service_client.list_containers()
    print("Conexión exitosa a Azure Blob Storage")
    for container in containers:
        print(container.name)
except Exception as e:
    print(f"Error de conexión: {e}")

# Nombres de los contenedores
photo_container_name = "fotos"
video_container_name = "videos"

# Configuración de Azure Video Indexer
video_indexer_api_key = os.getenv("VIDEO_INDEXER_API_KEY")
video_indexer_account_id = os.getenv("VIDEO_INDEXER_ACCOUNT_ID")
video_indexer_location = "trial"

# Obtener el token de Azure Video Indexer
def get_video_indexer_token():
    url = f"https://api.videoindexer.ai/Auth/{video_indexer_location}/Accounts/{video_indexer_account_id}/AccessToken"
    headers = {"Ocp-Apim-Subscription-Key": video_indexer_api_key}
    response = requests.get(url, headers=headers)
    return response.text.strip('"')

# Subir videos a Azure Video Indexer
def upload_to_video_indexer(blob_url, video_name):
    try:
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
    except Exception as e:
        print(f"Error en upload_to_video_indexer: {e}")

# Carpeta monitoreada
monitored_folder = os.path.join(os.path.dirname(__file__), "media")
if not os.path.exists(monitored_folder):
    os.makedirs(monitored_folder)

# Manejador de eventos para subir archivos
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            try:
                # Selección de contenedor basado en el tipo de archivo
                if file_name.endswith(('.jpg', '.jpeg', '.png')):
                    container_client = blob_service_client.get_container_client(photo_container_name)
                elif file_name.endswith(('.mp4', '.avi', '.mov')):
                    container_client = blob_service_client.get_container_client(video_container_name)
                else:
                    print("Tipo de archivo no compatible")
                    return
                
                # Subir archivo y obtener URL del blob
                with open(file_path, "rb") as data:
                    blob_client = container_client.upload_blob(name=file_name, data=data)
                    print(f"Archivo '{file_name}' subido a '{container_client.container_name}'.")

                blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_client.container_name}/{file_name}"

                # Enviar el video a Azure Video Indexer si es un archivo de video
                if file_name.endswith(('.mp4', '.avi', '.mov')):
                    upload_to_video_indexer(blob_url, file_name)

                # Eliminar archivo local después de la subida
                os.remove(file_path)
                print(f"Archivo '{file_name}' eliminado localmente.")

            except Exception as e:
                print(f"Error al subir el archivo: {e}")

# Captura de foto
def capture_photo():
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    photo_path = os.path.join(monitored_folder, f"photo_{timestamp}.jpg")
    try:
        subprocess.run(["libcamera-still", "-o", photo_path], check=True)
        print(f"Foto capturada en: {photo_path}")
    except Exception as e:
        print(f"Error al capturar la foto: {e}")

# Captura de video con conversión
def capture_video(duration):
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    h264_path = os.path.join(monitored_folder, f"video_{timestamp}.h264")
    video_path = os.path.join(monitored_folder, f"video_{timestamp}.mp4")
    try:
        subprocess.run(["libcamera-vid", "-t", str(duration * 1000), "-o", h264_path], check=True)
        conversion_command = f"ffmpeg -i {h264_path} -c:v libx264 -preset fast -crf 22 {video_path}"
        subprocess.run(conversion_command, shell=True, check=True)
        os.remove(h264_path)
        print(f"Video convertido a mp4: {video_path}")
    except Exception as e:
        print(f"Error al capturar o convertir el video: {e}")

# Configuración del observador de archivos
event_handler = FileHandler()
observer = Observer()
observer.schedule(event_handler, monitored_folder, recursive=False)
observer.start()

# Bucle principal para monitorear sensores
try:
    while True:
        try:
            temperature = KY_015.temperature
            humidity = KY_015.humidity
            if temperature is not None and humidity is not None:
                print(f"Temperatura: {temperature}°C, Humedad: {humidity}%")
            else:
                print("Error de lectura de temperatura/humedad")
        except Exception as e:
            print(f"Error al leer el sensor DHT11: {e}")
        
        try:
            sonido = gpio.input(SOUND_PIN)
            if sonido == 0:
                print("Silencio")
            elif sonido == 1:
                print("Sonido detectado")
        except Exception as e:
            print(f"Error en la detección de sonido: {e}")

        time.sleep(4)
        
        capture_photo()
        time.sleep(5)
        
        capture_video(25)
        time.sleep(30)

except KeyboardInterrupt:
    observer.stop()
finally:
    gpio.cleanup()
    observer.join()