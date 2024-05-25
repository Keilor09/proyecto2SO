import grpc
import threading
import time
import random
import messagebroker_pb2
import messagebroker_pb2_grpc
from google.protobuf.empty_pb2 import Empty

def publish_messages(stub, topic):
    for i in range(10):
        message_content = f"Mensaje {i} en {topic}"
        message = messagebroker_pb2.Message(content=message_content, timestamp=time.strftime("%d/%m/%Y:%H:%M:%S"))
        request = messagebroker_pb2.PublishRequest(topic=topic, message=message)
        response = stub.Publish(request)
        if response.success:
            print(f"Publicado: {message_content}")
        else:
            print(f"Error al publicar: {message_content}")
        time.sleep(random.uniform(0.1, 1.0))

def subscribe_to_topic(stub, topic):
    request = messagebroker_pb2.SubscribeRequest(topic=topic)
    try:
        for response in stub.Subscribe(request):
            print(f"Recibido del tema {topic}: {response.message.content} at {response.message.timestamp}")
    except grpc.RpcError as e:
        print(f"Error al suscribirse al tema {topic}: {e.details()}")

def test_multithreading():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = messagebroker_pb2_grpc.MessageBrokerStub(channel)

        topics = ["Futbol", "Natacion", "Atletismo"]
        threads = []

        # Lanzar hilos de publicación
        for topic in topics:
            t = threading.Thread(target=publish_messages, args=(stub, topic))
            threads.append(t)
            t.start()

        # Lanzar hilos de suscripción
        for topic in topics:
            t = threading.Thread(target=subscribe_to_topic, args=(stub, topic))
            threads.append(t)
            t.start()

        # Esperar a que todos los hilos terminen
        for t in threads:
            t.join()

if __name__ == '__main__':
    test_multithreading()
