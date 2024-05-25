import grpc
import threading
import time
import messagebroker_pb2
import messagebroker_pb2_grpc
from google.protobuf.empty_pb2 import Empty

def publish_message(stub, topic, message_content):
    message = messagebroker_pb2.Message(content=message_content, timestamp=time.strftime("%d/%m/%Y:%H:%M:%S"))
    request = messagebroker_pb2.PublishRequest(topic=topic, message=message)
    response = stub.Publish(request)
    if response.success:
        print(f"Mensaje publicado en el tema {topic}")
    else:
        print(f"Error al publicar el mensaje en el tema {topic}")

def subscribe_to_topic(stub, topic):
    request = messagebroker_pb2.SubscribeRequest(topic=topic)
    try:
        for response in stub.Subscribe(request):
            print(f"Mensaje recibido del tema {topic}: {response.message.content} a las {response.message.timestamp}")
    except grpc.RpcError as e:
        print(f"Error al suscribirse al tema {topic}: {e.details()}")

def list_topics(stub):
    response = stub.ListTopics(Empty())
    print("Temas disponibles:")
    for topic in response.topics:
        print(f"- {topic}")

def get_subscribed_topics(stub):
    response = stub.GetSubscribedTopics(Empty())
    for topic in response.topics:
        print(f"Tema: {topic.name}")
        for message in topic.messages:
            print(f"  Mensaje: {message.content} a las {message.timestamp}")

def run():
    with grpc.insecure_channel('127.0.0.1:50051') as channel:
        stub = messagebroker_pb2_grpc.MessageBrokerStub(channel)
        
        while True:
            print("1. Publicar mensaje")
            print("2. Suscribirse a un tema")
            print("3. Mostrar todos los temas")
            print("4. Mostrar mis temas suscritos")
            print("5. Salir")
            option = input("Seleccione una opción: ")

            if option == '1':
                topic = input("Ingrese el nombre del tema: ")
                message_content = input("Ingrese el contenido del mensaje: ")
                publish_message(stub, topic, message_content)
            elif option == '2':
                topic = input("Ingrese el nombre del tema: ")
                subscriber_thread = threading.Thread(target=subscribe_to_topic, args=(stub, topic))
                subscriber_thread.start()
            elif option == '3':
                list_topics(stub)
            elif option == '4':
                get_subscribed_topics(stub)
            elif option == '5':
                print("Saliendo...")
                break
            else:
                print("Opción no válida, intente de nuevo.")

if __name__ == '__main__':
    run()
