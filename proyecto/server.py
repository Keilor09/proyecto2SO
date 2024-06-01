import grpc
from concurrent import futures
import threading
import queue
import time
import datetime
from google.protobuf.empty_pb2 import Empty
import messagebroker_pb2
import messagebroker_pb2_grpc

# Definicion de los temas y sus respectivas colas de mensajes
# Cada tema tiene una cola de tamaño maximo 3
TOPICS = {
    "Futbol": queue.Queue(maxsize=3),
    "Natacion": queue.Queue(maxsize=3),
    "Atletismo": queue.Queue(maxsize=3)
}

# Archivo de log para registrar eventos
LOG_FILE = 'server_log.txt'
log_lock = threading.Lock()  # Lock para evitar problemas de concurrencia al escribir en el archivo de log

# Funcion para escribir en el archivo de log
def log_event(message):
    timestamp = datetime.datetime.now().strftime("%d/%m/%Y:%H:%M:%S")  # Obtener la fecha y hora actual
    log_message = f"{timestamp} {message}\n"  # Formatear el mensaje de log con un timestamp
    with log_lock:  # Asegurarse de que sólo un hilo escriba en el log a la vez
        with open(LOG_FILE, 'a') as log_file:
            log_file.write(log_message)

# Clase que representa a un suscriptor
class Subscriber:
    def __init__(self, context, topic):
        self.queue = queue.Queue(maxsize=10)  # Cola de mensajes para el suscriptor
        self.context = context  # Contexto del gRPC para gestionar la conexion
        self.topic = topic  # Tema al que esta suscrito
        self.thread = threading.Thread(target=self._handle_subscriber)  # Hilo para manejar la recepcion de mensajes
        self.thread.daemon = True  # Hacer que el hilo sea un daemon (se cierra cuando el programa termina)
        self.thread.start()  # Iniciar el hilo

    # Funcion que maneja la recepcion de mensajes para el suscriptor
    def _handle_subscriber(self):
        while not self.context.is_active():  # Esperar a que la conexión este activa
            time.sleep(1)
        while self.context.is_active():  # Continuar mientras la conexión este activa
            try:
                message = self.queue.get(timeout=1)  # Obtener un mensaje de la cola con un timeout de 1 segundo
                self.context.write(messagebroker_pb2.SubscribeResponse(message=message))  # Enviar el mensaje al cliente
            except queue.Empty:
                continue

# Clase que implementa el servicio MessageBroker definido en el archivo .proto
class MessageBrokerServicer(messagebroker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.subscribers = {topic: [] for topic in TOPICS}  # Diccionario de suscriptores por tema
        self.subscriber_locks = {topic: threading.Lock() for topic in TOPICS}  # Locks para manejar concurrencia
        self.subscribed_topics = {}  # Diccionario para rastrear los temas a los que cada suscriptor esta suscrito

    # Metodo para publicar un mensaje en un tema
    def Publish(self, request, context):
        topic = request.topic
        message = request.message

        if topic not in TOPICS:  # Verificar si el tema existe
            return messagebroker_pb2.Acknowledge(success=False)

        try:
            TOPICS[topic].put_nowait(message)  # Intentar poner el mensaje en la cola sin bloquear
            log_event(f"Mensaje publicado en el tema {topic}")
        except queue.Full:  # Si la cola está llena, registrar un error
            log_event(f"Fallo al publicar mensaje en el tema {topic}: Cola llena")
            return messagebroker_pb2.Acknowledge(success=False)

        with self.subscriber_locks[topic]:  # Asegurarse de que no haya problemas de concurrencia
            for subscriber in self.subscribers[topic]:  # Enviar el mensaje a todos los suscriptores del tema
                subscriber.queue.put(message)
                log_event(f"Mensaje enviado a suscriptor en el tema {topic}")

        return messagebroker_pb2.Acknowledge(success=True)

    # Metodo para suscribirse a un tema
    def Subscribe(self, request, context):
        topic = request.topic

        if topic not in TOPICS:  # Verificar si el tema existe
            context.set_details("Tema no encontrado")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return messagebroker_pb2.SubscribeResponse()

        subscriber = Subscriber(context, topic)  # Crear un nuevo suscriptor

        with self.subscriber_locks[topic]:  # Asegurarse de que no haya problemas de concurrencia
            self.subscribers[topic].append(subscriber)  # Añadir el suscriptor a la lista de suscriptores del tema
            if context.peer() not in self.subscribed_topics:
                self.subscribed_topics[context.peer()] = []
            self.subscribed_topics[context.peer()].append(topic)  # Registrar el tema al que se ha suscrito el cliente
            log_event(f"Nuevo suscriptor en el tema {topic}")

        try:
            while True:
                message = subscriber.queue.get()  # Obtener mensajes de la cola del suscriptor
                yield messagebroker_pb2.SubscribeResponse(message=message)  # Enviar el mensaje al cliente
        except grpc.RpcError:  # Manejar posibles errores de RPC
            with self.subscriber_locks[topic]:
                self.subscribers[topic].remove(subscriber)  # Eliminar el suscriptor si hay un error
                log_event(f"Suscriptor eliminado del tema {topic}")

    # Metodo para listar todos los temas
    def ListTopics(self, request, context):
        return messagebroker_pb2.ListTopicsResponse(topics=list(TOPICS.keys())) # Crear una respuesta ListTopicsResponse que contiene una lista de los nombres de todos los temas

    # Metodo para obtener los temas a los que esta suscrito un cliente
    def GetSubscribedTopics(self, request, context):
        response = messagebroker_pb2.GetSubscribedTopicsResponse() # Crear una instancia de la respuesta GetSubscribedTopicsResponse
        if context.peer() in self.subscribed_topics:  # Verificar si el cliente (context.peer()) esta en la lista de suscriptores
            for topic in self.subscribed_topics[context.peer()]:  # Iterar sobre cada tema al que esta suscrito el cliente
                messages = list(TOPICS[topic].queue)  # Obtener todos los mensajes en la cola del tema y convertirlos en una lista
                subscribed_topic = messagebroker_pb2.SubscribedTopic(name=topic, messages=messages) # Crear una instancia de SubscribedTopic con el nombre del tema y los mensajes
                response.topics.append(subscribed_topic)  # Añadir el tema suscrito a la respuesta
                log_event(f"Enviando mensajes del tema {topic} a solicitud del suscriptor")   # Registrar en el log que los mensajes del tema han sido enviados a solicitud del suscriptor
        return response # Devolver la respuesta que contiene los temas a los que esta suscrito el cliente

# Funcion para iniciar el servidor
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # Crear un servidor gRPC con un pool de hilos
    messagebroker_pb2_grpc.add_MessageBrokerServicer_to_server(MessageBrokerServicer(), server)  # Añadir el servicio al servidor
    server.add_insecure_port('[::]:50051')  # Escuchar en el puerto 50051
    server.start()  # Iniciar el servidor
    log_event("Servidor iniciado en el puerto 50051")
    server.wait_for_termination()  # Esperar a que el servidor termine

if __name__ == '__main__':
    serve()  # Iniciar el servidor cuando se ejecuta el script
