import grpc
from concurrent import futures
import threading
import queue
import time
import datetime
from google.protobuf.empty_pb2 import Empty
import messagebroker_pb2
import messagebroker_pb2_grpc

# Definición de los temas y sus respectivas colas de mensajes
TOPICS = {
    "Futbol": queue.Queue(maxsize=3),
    "Natacion": queue.Queue(maxsize=3),
    "Atletismo": queue.Queue(maxsize=3)
}

# Archivo de log
LOG_FILE = 'server_log.txt'
log_lock = threading.Lock()

# Función para escribir en el archivo de log
def log_event(message):
    timestamp = datetime.datetime.now().strftime("%d/%m/%Y:%H:%M:%S")
    log_message = f"{timestamp} {message}\n"
    with log_lock:
        with open(LOG_FILE, 'a') as log_file:
            log_file.write(log_message)

class Subscriber:
    def __init__(self, context, topic):
        self.queue = queue.Queue(maxsize=10)
        self.context = context
        self.topic = topic
        self.thread = threading.Thread(target=self._handle_subscriber)
        self.thread.daemon = True
        self.thread.start()

    def _handle_subscriber(self):
        while not self.context.is_active():
            time.sleep(1)
        while self.context.is_active():
            try:
                message = self.queue.get(timeout=1)
                self.context.write(messagebroker_pb2.SubscribeResponse(message=message))
            except queue.Empty:
                continue

# Clase que implementa el servicio MessageBroker
class MessageBrokerServicer(messagebroker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.subscribers = {topic: [] for topic in TOPICS}
        self.subscriber_locks = {topic: threading.Lock() for topic in TOPICS}
        self.subscribed_topics = {}

    def Publish(self, request, context):
        topic = request.topic
        message = request.message

        if topic not in TOPICS:
            return messagebroker_pb2.Acknowledge(success=False)

        try:
            TOPICS[topic].put_nowait(message)
            log_event(f"Mensaje publicado en el tema {topic}")
        except queue.Full:
            log_event(f"Fallo al publicar mensaje en el tema {topic}: Cola llena")
            return messagebroker_pb2.Acknowledge(success=False)

        with self.subscriber_locks[topic]:
            for subscriber in self.subscribers[topic]:
                subscriber.queue.put(message)
                log_event(f"Mensaje enviado a suscriptor en el tema {topic}")

        return messagebroker_pb2.Acknowledge(success=True)

    def Subscribe(self, request, context):
        topic = request.topic

        if topic not in TOPICS:
            context.set_details("Topic not found")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return messagebroker_pb2.SubscribeResponse()

        subscriber = Subscriber(context, topic)

        with self.subscriber_locks[topic]:
            self.subscribers[topic].append(subscriber)
            if context.peer() not in self.subscribed_topics:
                self.subscribed_topics[context.peer()] = []
            self.subscribed_topics[context.peer()].append(topic)
            log_event(f"Nuevo suscriptor en el tema {topic}")

        try:
            while True:
                message = subscriber.queue.get()
                yield messagebroker_pb2.SubscribeResponse(message=message)
        except grpc.RpcError:
            with self.subscriber_locks[topic]:
                self.subscribers[topic].remove(subscriber)
                log_event(f"Suscriptor eliminado del tema {topic}")

    def ListTopics(self, request, context):
        return messagebroker_pb2.ListTopicsResponse(topics=list(TOPICS.keys()))

    def GetSubscribedTopics(self, request, context):
        response = messagebroker_pb2.GetSubscribedTopicsResponse()
        if context.peer() in self.subscribed_topics:
            for topic in self.subscribed_topics[context.peer()]:
                messages = list(TOPICS[topic].queue)
                subscribed_topic = messagebroker_pb2.SubscribedTopic(name=topic, messages=messages)
                response.topics.append(subscribed_topic)
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messagebroker_pb2_grpc.add_MessageBrokerServicer_to_server(MessageBrokerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    log_event("Servidor iniciado en el puerto 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
