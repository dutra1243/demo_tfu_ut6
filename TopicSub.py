import pika

def TopicSub():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declaramos el exchange topic
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    # Creamos colas exclusivas temporales para cada patrón de suscripción
    result_error = channel.queue_declare(queue='', exclusive=True)
    error_queue_name = result_error.method.queue

    result_info = channel.queue_declare(queue='', exclusive=True)
    info_queue_name = result_info.method.queue

    result_all_logs = channel.queue_declare(queue='', exclusive=True)
    all_logs_queue_name = result_all_logs.method.queue

    result_critical = channel.queue_declare(queue='', exclusive=True)
    critical_queue_name = result_critical.method.queue

    # Bindeamos las colas con diferentes patrones de routing keys
    # Patrón 1: Solo mensajes de error (*.error.*)
    channel.queue_bind(exchange='topic_logs', 
                      queue=error_queue_name, 
                      routing_key='*.error.*')

    # Patrón 2: Solo mensajes de info (*.info.*)
    channel.queue_bind(exchange='topic_logs', 
                      queue=info_queue_name, 
                      routing_key='*.info.*')

    # Patrón 3: Todos los logs (log.*)
    channel.queue_bind(exchange='topic_logs', 
                      queue=all_logs_queue_name, 
                      routing_key='log.*')

    # Patrón 4: Mensajes críticos del sistema (system.critical.*)
    channel.queue_bind(exchange='topic_logs', 
                      queue=critical_queue_name, 
                      routing_key='system.critical.*')

    # Funciones callback para cada tipo de mensaje
    def callback_error(ch, method, properties, body):
        print(f"[ERROR] Mensaje recibido: {body.decode()}")
        print(f"    Routing key: {method.routing_key}")

    def callback_info(ch, method, properties, body):
        print(f"[INFO] Mensaje recibido: {body.decode()}")
        print(f"    Routing key: {method.routing_key}")

    def callback_all_logs(ch, method, properties, body):
        print(f"[ALL LOGS] Mensaje recibido: {body.decode()}")
        print(f"    Routing key: {method.routing_key}")

    def callback_critical(ch, method, properties, body):
        print(f"[CRITICAL] Mensaje recibido: {body.decode()}")
        print(f"    Routing key: {method.routing_key}")

    # Registramos los consumidores para cada cola
    channel.basic_consume(queue=error_queue_name,
                         on_message_callback=callback_error,
                         auto_ack=True)

    channel.basic_consume(queue=info_queue_name,
                         on_message_callback=callback_info,
                         auto_ack=True)

    channel.basic_consume(queue=all_logs_queue_name,
                         on_message_callback=callback_all_logs,
                         auto_ack=True)

    channel.basic_consume(queue=critical_queue_name,
                         on_message_callback=callback_critical,
                         auto_ack=True)

    print("Topic Subscriber iniciado!")
    print("Patrones de suscripción:")
    print("  - *.error.*: Todos los mensajes de error")
    print("  - *.info.*: Todos los mensajes de información")
    print("  - log.*: Todos los logs del sistema")
    print("  - system.critical.*: Mensajes críticos del sistema")
    print("\nEsperando mensajes. Ctrl+C para salir.")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDeteniendo el consumidor...")
        channel.stop_consuming()
        connection.close()


TopicSub()
