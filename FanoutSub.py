import pika

def FanoutSub():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declaramos el exchange fanout
    channel.exchange_declare(exchange='fanout', exchange_type='fanout')

    # Declaramos la cola que recibirá los mensajes (con nombre único)
    channel.queue_declare(queue='fanout_subscriber')

    # Aseguramos que exista la misma cola
    channel.queue_bind(exchange='fanout', queue='fanout_subscriber', routing_key='')

    # Función que se ejecuta cuando llega un mensaje
    def callback(ch, method, properties, body):
        print(f"Mensaje recibido: {body.decode()}")

    # Registramos el consumidor
    channel.basic_consume(queue='fanout_subscriber',
                        on_message_callback=callback,
                        auto_ack=True)
    
    print("Fanout Subscriber iniciado!")
    print("Esperando mensajes. Ctrl+C para salir.")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDeteniendo el consumidor...")
        channel.stop_consuming()
        connection.close()

FanoutSub()