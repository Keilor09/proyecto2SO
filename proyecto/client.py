import grpc
import threading
import time
import messagebroker_pb2
import messagebroker_pb2_grpc
from google.protobuf.empty_pb2 import Empty

# Funcion para publicar un mensaje en un tema
def publish_message(stub, topic, message_content):
    try:
        message = messagebroker_pb2.Message(content=message_content, timestamp=time.strftime("%d/%m/%Y:%H:%M:%S")) # Crear un mensaje con contenido y timestamp
        request = messagebroker_pb2.PublishRequest(topic=topic, message=message) # Crear una solicitud de publicacion con el tema y el mensaje
        response = stub.Publish(request) # Llamar al metodo Publish del stub
        
        # Verificar la respuesta y mostrar un mensaje adecuado
        if response.success:
            print(f"Mensaje publicado en el tema {topic}")
        else:
            print(f"Error al publicar el mensaje en el tema {topic}")
    except grpc.RpcError as e:
        print(f"Error al publicar mensaje: {e.details()}") # Manejar errores de RPC y mostrar un mensaje de error

# Funcion para suscribirse a un tema
def subscribe_to_topic(stub, topic):
    request = messagebroker_pb2.SubscribeRequest(topic=topic) # Crear una solicitud de suscripcion con el tema
    
    try:
        for response in stub.Subscribe(request): # Llamar al metodo Subscribe del stub y recibir mensajes en un bucle
            print(f"Mensaje recibido del tema {topic}: {response.message.content} a las {response.message.timestamp}")
    except grpc.RpcError as e:
        print(f"Error al suscribirse al tema {topic}: {e.details()}") # Manejar errores de RPC y mostrar un mensaje de error

# Funcion para listar todos los temas
def list_topics(stub):
    try:
        response = stub.ListTopics(Empty()) # Llamar al metodo ListTopics del stub y recibir la respuesta
        # Mostrar todos los temas disponibles
        print("Temas disponibles:")
        for topic in response.topics:
            print(f"- {topic}")
    except grpc.RpcError as e:
        print(f"Error al listar temas: {e.details()}") # Manejar errores de RPC y mostrar un mensaje de error

# Funcion para obtener los temas a los que esta suscrito el cliente
def get_subscribed_topics(stub):
    try:
        # Llamar al metodo GetSubscribedTopics del stub y recibir la respuesta
        response = stub.GetSubscribedTopics(Empty())
        
        # Mostrar todos los temas y mensajes a los que esta suscrito el cliente
        for topic in response.topics:
            print(f"Tema: {topic.name}")
            for message in topic.messages:
                print(f"  Mensaje: {message.content} a las {message.timestamp}")
    except grpc.RpcError as e:
        print(f"Error al obtener temas suscritos: {e.details()}")

# Funcion principal que corre el cliente
def run():
    # Crear un canal para comunicarse con el servidor gRPC
    with grpc.insecure_channel('127.0.0.1:50051') as channel:
        # Crear un stub para el servicio MessageBroker
        stub = messagebroker_pb2_grpc.MessageBrokerStub(channel)
        
        # Bucle principal del cliente
        while True:
            # Mostrar el menu de opciones
            print("1. Publicar mensaje")
            print("2. Suscribirse a un tema")
            print("3. Mostrar todos los temas")
            print("4. Mostrar mis temas suscritos")
            print("5. Salir")
            option = input("Seleccione una opción: ")

            if option == '1':
                # Publicar un mensaje en un tema
                print ("TEMAS: Futbol, Atletismo y Natacion")
                topic = input("Ingrese el nombre del tema: ")
                message_content = input("Ingrese el contenido del mensaje: ")
                publish_message(stub, topic, message_content)
            elif option == '2':
                # Suscribirse a un tema
                print ("TEMAS: Futbol, Atletismo y Natacion")
                topic = input("Ingrese el nombre del tema: ")
                subscriber_thread = threading.Thread(target=subscribe_to_topic, args=(stub, topic))
                subscriber_thread.start()
            elif option == '3':
                # Mostrar todos los temas disponibles
                list_topics(stub)
            elif option == '4':
                # Mostrar los temas a los que esta suscrito el cliente
                get_subscribed_topics(stub)
            elif option == '5':
                # Salir del programa
                print("Saliendo...")
                break
            else:
                # Manejar opcion no valida
                print("Opción no valida, intente de nuevo.")

# Main
if __name__ == '__main__':
    run()
