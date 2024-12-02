import os
import cv2
import threading
from flask import Flask, Response
from picamera2 import Picamera2
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

stream_ip = os.getenv("RASP_IP")
stream_port = os.getenv("STREAM_PORT") 
stream_url = (f"https://{stream_ip}:{stream_port}/stream")

# Configurar conexiones a Azure
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
iot_connection_string = os.getenv("AZURE_IOT_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
iot_client = IoTHubDeviceClient.create_from_connection_string(iot_connection_string)
MSG_TEMPLATE = '{{"Temperatura": {temperature}, "Humedad": {humidity}}}'

app = Flask(__name__) #Cargar aplicación de flask

# Variable para controlar el estado del stream Flask
is_streaming = False
flask_thread = None

# Configurar la cámara
picam2 = Picamera2()
picam2.configure(picam2.create_video_configuration(main={"size": (640, 480)}))
picam2.start()

# Inicializar el sensor DHT11 y el pin de sonido
KY_015 = adafruit_dht.DHT11(board.D4)
SOUND_PIN = 17
gpio.setmode(gpio.BCM)
gpio.setup(SOUND_PIN, gpio.IN)

# Configuración de carpetas y contenedores
monitored_folder = os.path.join(os.path.dirname(__file__), "media")
os.makedirs(monitored_folder, exist_ok=True)
photo_container = "fotos"
video_container = "videos"

# Función para notificar sobre el stream
def notify_iothub_about_stream(stream_url):
    try:
        iot_client.connect()
        message = Message(f'{{"stream_URL": "{stream_url}"}}')
        iot_client.send_message(message)
        print(f"Notificación enviada a IoT Hub: {stream_url}")
    except Exception as e:
        print(f"Error al notificar a IoT Hub: {e}")
        if hasattr(e, 'details'):
            print(f"Detalles del error: {e.details}")

# Transmisión MJPEG en Flask
def generate_frames():
    while True:
        frame = picam2.capture_array()
        frame_bgr = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
        _, buffer = cv2.imencode('.jpg', frame_bgr)
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + buffer.tobytes() + b'\r\n')
        
@app.route('/stream')
def video():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

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
        time.sleep(5)
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

def send_to_iothubsound(msg):
    try:
        msg = format(msg)
        iot_client.send_message(Message(msg))
        print(f"Mensaje enviado: {msg}")
    except Exception as e:
        print(f"Error al enviar mensaje a IoT Hub: {e}")

# Clase para manejar eventos de archivo
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        file_path = event.src_path
        if file_path.endswith(('.jpg', '.jpeg', '.png')):
            upload_to_blob(file_path, photo_container)

# Configuración de observador de archivos
event_handler = FileHandler()
observer = Observer()
observer.schedule(event_handler, monitored_folder, recursive=False)
observer.start()

def start_flask_stream():
    global is_streaming, flask_thread
    if not is_streaming:
        flask_thread = threading.Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": 5004, "threaded": True}, daemon=True)
        flask_thread.start()
        is_streaming = True
        print("Stream Flask iniciado.")

# Bucle principal
try:
    while True:
        temp, hum = capture_dht()
        sonido = gpio.input(SOUND_PIN)
        if temp is not None and hum is not None:
            print(f"Temperatura: {temp}°C, Humedad: {hum}%")
            if temp > 28:
                send_to_iothub(temp, hum)

        if sonido == 1:
            msg = "Sonido detectado"
            send_to_iothubsound(msg)
        else:
            print("Silencio")
        
        if not is_streaming:
            start_flask_stream()
            notify_iothub_about_stream(stream_url)
        time.sleep(5)
except KeyboardInterrupt:
    print("Interrumpido por el usuario.")
    gpio.cleanup()
finally:
    gpio.cleanup()
    observer.stop()
    observer.join()