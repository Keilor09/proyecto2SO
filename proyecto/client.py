import grpc 
import threading 
import time 
import messagebroker_pb2 
import messagebroker_pb2_grpc  
from google.protobuf.empty_pb2 import Empty 

# Funcion para publicar un mensaje en un tema
def publicar_mensaje(stub, tema, contenido_mensaje):
    try:
        # Crea un mensaje con contenido y marca de tiempo actual
        mensaje = messagebroker_pb2.Mensaje(contenido=contenido_mensaje, marca_de_tiempo=time.strftime("%d/%m/%Y:%H:%M:%S"))
        # Crea una solicitud de publicación con el tema y el mensaje
        solicitud = messagebroker_pb2.PublicarSolicitud(tema=tema, mensaje=mensaje)
        # Llama al metodo Publicar del stub y recibe la respuesta
        respuesta = stub.Publicar(solicitud)
        # Verifica si la publicacion fue exitosa
        if respuesta.exito:
            print(f"Mensaje publicado en el tema {tema}")
        else:
            print(f"Error al publicar el mensaje en el tema {tema}")
    except grpc.RpcError as e:
        # Maneja errores de RPC y muestra un mensaje de error
        print(f"Error al publicar mensaje: {e.details()}")

# Funcion para suscribirse a un tema
def suscribirse_a_tema(stub, tema):
    # Crea una solicitud de suscripcion con el tema
    solicitud = messagebroker_pb2.SuscribirSolicitud(tema=tema)
    try:
        # Llama al metodo Suscribir del stub y recibe mensajes en un bucle
        for respuesta in stub.Suscribir(solicitud):
            print(f"Mensaje recibido del tema {tema}: {respuesta.mensaje.contenido} a las {respuesta.mensaje.marca_de_tiempo}")
    except grpc.RpcError as e:
        # Maneja errores de RPC y muestra un mensaje de error
        print(f"Error al suscribirse al tema {tema}: {e.details()}")

# Funcion para listar todos los temas
def listar_temas(stub):
    try:
        # Llama al metodo ListarTemas del stub y recibe la respuesta
        respuesta = stub.ListarTemas(Empty())
        # Muestra todos los temas disponibles
        print("Temas disponibles:")
        for tema in respuesta.temas:
            print(f"- {tema}")
    except grpc.RpcError as e:
        # Maneja errores de RPC y muestra un mensaje de error
        print(f"Error al listar temas: {e.details()}")

# Funcion para obtener los temas a los que está suscrito el cliente
def obtener_temas_suscritos(stub):
    try:
        # Llama al metodo ObtenerTemasSuscritos del stub y recibe la respuesta
        respuesta = stub.ObtenerTemasSuscritos(Empty())
        # Muestra todos los temas y mensajes a los que está suscrito el cliente
        for tema_suscrito in respuesta.temas:
            print(f"Tema: {tema_suscrito.nombre}")
            for mensaje in tema_suscrito.mensajes:
                print(f"  Mensaje: {mensaje.contenido} a las {mensaje.marca_de_tiempo}")
    except grpc.RpcError as e:
        # Maneja errores de RPC y muestra un mensaje de error
        print(f"Error al obtener temas suscritos: {e.details()}")

# Funcion principal que corre el cliente
def run():
    # Crear un canal para comunicarse con el servidor gRPC
    with grpc.insecure_channel('127.0.0.1:50051') as channel:
        # Crear un stub para el servicio IntermediarioMensajes
        stub = messagebroker_pb2_grpc.MessageBrokerStub(channel)
        # Bucle principal del cliente
        while True:
            # Mostrar el menú de opciones
            print("1. Publicar mensaje")
            print("2. Suscribirse a un tema")
            print("3. Mostrar todos los temas")
            print("4. Mostrar mis temas suscritos")
            print("5. Salir")
            opcion = input("Seleccione una opción: ")

            if opcion == '1':
                # Publicar un mensaje en un tema
                print("TEMAS: Futbol, Atletismo y Natacion")
                tema = input("Ingrese el nombre del tema: ")
                contenido_mensaje = input("Ingrese el contenido del mensaje: ")
                publicar_mensaje(stub, tema, contenido_mensaje)
            elif opcion == '2':
                # Suscribirse a un tema
                print("TEMAS: Futbol, Atletismo y Natacion")
                tema = input("Ingrese el nombre del tema: ")
                suscriptor_thread = threading.Thread(target=suscribirse_a_tema, args=(stub, tema))
                suscriptor_thread.start()
            elif opcion == '3':
                # Mostrar todos los temas disponibles
                listar_temas(stub)
            elif opcion == '4':
                # Mostrar los temas a los que esta suscrito el cliente
                obtener_temas_suscritos(stub)
            elif opcion == '5':
                # Salir del programa
                print("Saliendo...")
                break
            else:
                # Manejar opcion no válida
                print("Opción no válida, intente de nuevo.")

# Main
if __name__ == '__main__':
    run()  # Ejecuta la funcion principal cuando se ejecuta el script
