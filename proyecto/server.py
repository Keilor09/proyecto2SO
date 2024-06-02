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
TEMAS = {
    "Futbol": queue.Queue(maxsize=3),
    "Natacion": queue.Queue(maxsize=3),
    "Atletismo": queue.Queue(maxsize=3)
}

# Archivo de log para registrar eventos
ARCHIVO_LOG = 'server_log.txt'
log_lock = threading.Lock()  # Lock para evitar problemas de concurrencia al escribir en el archivo de log

# Funcion para escribir en el archivo de log
def registrar_evento(mensaje):
    marca_de_tiempo = datetime.datetime.now().strftime("%d/%m/%Y:%H:%M:%S")  # Obtener la fecha y hora actual
    log_message = f"{marca_de_tiempo} {mensaje}\n"  # Formatear el mensaje de log con un timestamp
    with log_lock:  # Asegurarse de que sólo un hilo escriba en el log a la vez
        with open(ARCHIVO_LOG, 'a') as log_file:
            log_file.write(log_message)

# Clase que representa a un suscriptor
class Suscriptor:
    def __init__(self, context, tema):
        self.queue = queue.Queue(maxsize=10)  # Cola de mensajes para el suscriptor
        self.context = context  # Contexto del gRPC para gestionar la conexion
        self.tema = tema  # Tema al que esta suscrito
        self.thread = threading.Thread(target=self.manejar_suscriptor)  # Hilo para manejar la recepcion de mensajes
        self.thread.daemon = True  # Hacer que el hilo sea un daemon (se cierra cuando el programa termina)
        self.thread.start()  # Iniciar el hilo

    # Funcion que maneja la recepcion de mensajes para el suscriptor
    def manejar_suscriptor(self):
        while not self.context.is_active():  # Esperar a que la conexión este activa
            time.sleep(1)
        while self.context.is_active():  # Continuar mientras la conexión este activa
            try:
                mensaje = self.queue.get(timeout=1)  # Obtener un mensaje de la cola con un timeout de 1 segundo
                self.context.write(messagebroker_pb2.SuscribirRespuesta(mensaje=mensaje))  # Enviar el mensaje al cliente
            except queue.Empty:
                continue

# Clase que implementa el servicio IntermediarioMensajes definido en el archivo .proto
class messagebrokerServicer(messagebroker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.suscriptores = {tema: [] for tema in TEMAS}  # Diccionario de suscriptores por tema
        self.suscriptor_locks = {tema: threading.Lock() for tema in TEMAS}  # Locks para manejar concurrencia
        self.temas_suscritos = {}  # Diccionario para rastrear los temas a los que cada suscriptor esta suscrito

    # Metodo para publicar un mensaje en un tema
    def Publicar(self, request, context):
        tema = request.tema
        mensaje = request.mensaje

        if tema not in TEMAS:  # Verificar si el tema existe
            return messagebroker_pb2.Acknowledge(exito=False)

        try:
            TEMAS[tema].put_nowait(mensaje)  # Intentar poner el mensaje en la cola sin bloquear
            registrar_evento(f"Mensaje publicado en el tema {tema}")
        except queue.Full:  # Si la cola está llena, registrar un error
            registrar_evento(f"Fallo al publicar mensaje en el tema {tema}: Cola llena")
            return messagebroker_pb2.Acknowledge(exito=False)

        with self.suscriptor_locks[tema]:  # Asegurarse de que no haya problemas de concurrencia
            for suscriptor in self.suscriptores[tema]:  # Enviar el mensaje a todos los suscriptores del tema
                suscriptor.queue.put(mensaje)
                registrar_evento(f"Mensaje enviado a suscriptor en el tema {tema}")

        return messagebroker_pb2.Acknowledge(exito=True)

    # Metodo para suscribirse a un tema
    def Suscribir(self, request, context):
        tema = request.tema

        if tema not in TEMAS:  # Verificar si el tema existe
            context.set_details("Tema no encontrado")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return messagebroker_pb2.SuscribirRespuesta()

        suscriptor = Suscriptor(context, tema)  # Crear un nuevo suscriptor

        with self.suscriptor_locks[tema]:  # Asegurarse de que no haya problemas de concurrencia
            self.suscriptores[tema].append(suscriptor)  # Añadir el suscriptor a la lista de suscriptores del tema
            if context.peer() not in self.temas_suscritos:
                self.temas_suscritos[context.peer()] = []
            self.temas_suscritos[context.peer()].append(tema)  # Registrar el tema al que se ha suscrito el cliente
            registrar_evento(f"Nuevo suscriptor en el tema {tema}")

        try:
            while True:
                mensaje = suscriptor.queue.get()  # Obtener mensajes de la cola del suscriptor
                yield messagebroker_pb2.SuscribirRespuesta(mensaje=mensaje)  # Enviar el mensaje al cliente
        except grpc.RpcError:  # Manejar posibles errores de RPC
            with self.suscriptor_locks[tema]:
                self.suscriptores[tema].remove(suscriptor)  # Eliminar el suscriptor si hay un error
                registrar_evento(f"Suscriptor eliminado del tema {tema}")

    # Metodo para listar todos los temas
    def ListarTemas(self, request, context):
        return messagebroker_pb2.ListarTemasRespuesta(temas=list(TEMAS.keys()))

    # Metodo para obtener los temas a los que esta suscrito un cliente
    def ObtenerTemasSuscritos(self, request, context):
        respuesta = messagebroker_pb2.ObtenerTemasSuscritosRespuesta()
        if context.peer() in self.temas_suscritos:  # Verificar si el cliente (context.peer()) esta en la lista de suscriptores
            for tema in self.temas_suscritos[context.peer()]:  # Iterar sobre cada tema al que esta suscrito el cliente
                mensajes = list(TEMAS[tema].queue)  # Obtener todos los mensajes en la cola del tema y convertirlos en una lista
                tema_suscrito = messagebroker_pb2.TemaSuscrito(nombre=tema, mensajes=mensajes)  # Crear una instancia de TemaSuscrito con el nombre del tema y los mensajes
                respuesta.temas.append(tema_suscrito)  # Añadir el tema suscrito a la respuesta
                registrar_evento(f"Enviando mensajes del tema {tema} a solicitud del suscriptor")  # Registrar en el log que los mensajes del tema han sido enviados a solicitud del suscriptor
        return respuesta  # Devolver la respuesta que contiene los temas a los que esta suscrito el cliente

# Funcion para iniciar el servidor
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # Crear un servidor gRPC con un pool de hilos
    messagebroker_pb2_grpc.add_MessageBrokerServicer_to_server(messagebrokerServicer(), server)  # Añadir el servicio al servidor
    server.add_insecure_port('[::]:50051')  # Escuchar en el puerto 50051
    server.start()  # Iniciar el servidor
    registrar_evento("Servidor iniciado en el puerto 50051")
    server.wait_for_termination()  # Esperar a que el servidor termine

if __name__ == '__main__':
    serve()  # Iniciar el servidor cuando se ejecuta el script
