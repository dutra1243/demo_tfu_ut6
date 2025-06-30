import pika

def DirectSub():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declaramos los exchanges (deben existir primero)
    channel.exchange_declare(exchange='tareas', exchange_type='direct')
    channel.exchange_declare(exchange='fanout', exchange_type='fanout')

    # Declaramos las colas
    channel.queue_declare(queue='tareas')
    channel.queue_declare(queue='fanout')

    # Bindeamos las colas a los exchanges
    channel.queue_bind(exchange='tareas', queue='tareas', routing_key='seba')
    channel.queue_bind(exchange='fanout', queue='fanout', routing_key='')

    # Función que se ejecuta cuando llega un mensaje
    def callback_direct(ch, method, properties, body):
        print(f"Mensaje recibido (direct): {body.decode()}")

    def callback_fanout(ch, method, properties, body):
        print(f"Mensaje recibido (fanout): {body.decode()}")

    # Registramos el consumidor
    channel.basic_consume(queue='tareas',
                        on_message_callback=callback_direct,
                        auto_ack=True)
    
    channel.basic_consume(queue='fanout',
                        on_message_callback=callback_fanout,
                        auto_ack=True)

    print("Direct Subscriber iniciado!")
    print("Esperando mensajes. Ctrl+C para salir.")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDeteniendo el consumidor...")
        channel.stop_consuming()
        connection.close()

DirectSub()