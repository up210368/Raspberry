import os
import cv2
import threading
from dotenv import load_dotenv
import time
import subprocess
import adafruit_dht
import board
import RPi.GPIO as gpio
from azure.storage.blob import BlobServiceClient
from azure.iot.device import IoTHubDeviceClient, Message
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Cargar variables de entorno
load_dotenv()

RTSP_PORT = os.getenv("RTSP_PORT")
RTSP_STREAM_NAME = "fiera-live"
RASP_IP = os.getenv("RASP_IP")
RTSP_URL = f"rtsp://{RASP_IP}:{RTSP_PORT}/{RTSP_STREAM_NAME}"
rtsp_thread = None
def start_rtsp_stream():
    """
    Captura video desde la cámara y transmite a través de un servidor RTSP.
    """
    try:
        print("Iniciando transmisión RTSP...")

        # Capturar video de la cámara (ID 0)
        cap = cv2.VideoCapture(0)
        if not cap.isOpened():
            raise Exception("No se pudo acceder a la cámara.")

        # Parámetros de la cámara
        width, height, fps = 640, 480, 30
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
        cap.set(cv2.CAP_PROP_FPS, fps)

        # Configurar el servidor RTSP con ffmpeg
        rtsp_command = (
            f"ffmpeg -re -f rawvideo -pix_fmt bgr24 -s {width}x{height} "
            f"-r {fps} -i pipe:0 -c:v libx264 -preset ultrafast -tune zerolatency "
            f"-f rtsp rtsp://{RASP_IP}:{RTSP_PORT}/{RTSP_STREAM_NAME}"
        )

        process = subprocess.Popen(rtsp_command, shell=True, stdin=subprocess.PIPE)

        while True:
            # Leer fotogramas de la cámara
            ret, frame = cap.read()
            if not ret:
                break

            # Escribir fotogramas al proceso de ffmpeg
            process.stdin.write(frame.tobytes())

    except Exception as e:
        print(f"Error al iniciar la transmisión RTSP: {e}")
    finally:
        cap.release()
        process.stdin.close()
        process.terminate()

def notify_iothub_about_stream():
    """
    Notifica a Azure IoT Hub sobre la disponibilidad del stream RTSP.
    """
    try:
        message = Message(f'{{"RTSP_URL": "{RTSP_URL}"}}')
        iot_client.send_message(message)
        print(f"Notificación enviada a IoT Hub: {RTSP_URL}")
    except Exception as e:
        print(f"Error al notificar a IoT Hub: {e}")

def stop_rtsp_stream():
    """
    Detiene el servidor RTSP.
    """
    try:
        print("Deteniendo transmisión RTSP...")
        subprocess.run(["pkill", "-f", "libcamera-vid"], check=True)
    except Exception as e:
        print(f"Error al detener la transmisión RTSP: {e}")

# Inicializar el sensor DHT11 y el pin de sonido
KY_015 = adafruit_dht.DHT11(board.D4)
SOUND_PIN = 17
gpio.setmode(gpio.BCM)
gpio.setup(SOUND_PIN, gpio.IN)

# Configurar conexiones a Azure
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
iot_connection_string = os.getenv("AZURE_IOT_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
iot_client = IoTHubDeviceClient.create_from_connection_string(iot_connection_string)
MSG_TEMPLATE = '{{"Temperatura": {temperature}, "Humedad": {humidity}}}'

# Configuración de carpetas y contenedores
monitored_folder = os.path.join(os.path.dirname(__file__), "media")
os.makedirs(monitored_folder, exist_ok=True)
photo_container = "fotos"
video_container = "videos"

# Función para subir archivos
def upload_to_blob(file_path, container_name):
    file_name = os.path.basename(file_path)
    try:
        container_client = blob_service_client.get_container_client(container_name)
        with open(file_path, "rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)
        print(f"Archivo '{file_name}' subido a contenedor '{container_name}'.")
        os.remove(file_path)
    except Exception as e:
        print(f"Error al subir archivo '{file_name}': {e}")

# Clase para manejar eventos de archivo
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        file_path = event.src_path
        if file_path.endswith(('.jpg', '.jpeg', '.png')):
            upload_to_blob(file_path, photo_container)

# Captura de fotos
def capture_photo():
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    photo_path = os.path.join(monitored_folder, f"photo_{timestamp}.jpg")
    try:
        subprocess.run(["libcamera-still", "-o", photo_path], check=True)
        print(f"Foto capturada: {photo_path}")
    except Exception as e:
        print(f"Error al capturar foto: {e}")

# Captura y subida de video
def capture_video(duration):
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    h264_path = os.path.join(monitored_folder, f"video_{timestamp}.h264")
    video_path = os.path.join(monitored_folder, f"video_{timestamp}.mp4")
    try:
        subprocess.run(["libcamera-vid", "-t", str(duration * 1000), "-o", h264_path], check=True)
        subprocess.run(f"ffmpeg -i {h264_path} -c:v libx264 -preset fast -crf 22 {video_path}", shell=True, check=True)
        os.remove(h264_path)
        print(f"Video convertido: {video_path}")
        time.sleep(7)
        upload_to_blob(video_path, video_container)
    except Exception as e:
        print(f"Error al capturar video: {e}")

# Captura de datos del sensor DHT11
def capture_dht():
    try:
        temperature = KY_015.temperature
        humidity = KY_015.humidity
        return temperature, humidity
    except Exception as e:
        print(f"Error al leer sensor DHT11: {e}")
        return None, None

# Enviar datos a Azure IoT Hub
def send_to_iothub(temperature, humidity):
    try:
        msg_formatted = MSG_TEMPLATE.format(temperature=temperature, humidity=humidity)
        iot_client.send_message(Message(msg_formatted))
        print(f"Mensaje enviado: {msg_formatted}")
    except Exception as e:
        print(f"Error al enviar mensaje a IoT Hub: {e}")

# Configuración de observador de archivos
event_handler = FileHandler()
observer = Observer()
observer.schedule(event_handler, monitored_folder, recursive=False)
observer.start()

# Bucle principal
try:
    while True:
        temp, hum = capture_dht()
        sonido = gpio.input(SOUND_PIN)
        if temp is not None and hum is not None:
            print(f"Temperatura: {temp}°C, Humedad: {hum}%")
            send_to_iothub(temp, hum)

        if sonido == 1:
            print("Sonido detectado")
            if rtsp_thread is None or not rtsp_thread.is_alive():
                rtsp_thread = threading.Thread(target=start_rtsp_stream, daemon=True)
                rtsp_thread.start()
                notify_iothub_about_stream()
        else:
            print("Silencio")
        
        time.sleep(50)
        capture_photo()
        time.sleep(2)
        capture_video(15)
        time.sleep(30)
except KeyboardInterrupt:
    print("Interrumpido por el usuario.")
    if rtsp_thread and rtsp_thread.is_alive():
        pass
    stop_rtsp_stream()
finally:
    gpio.cleanup()
    stop_rtsp_stream()
    observer.stop()
    observer.join()