import pika

def DirectPub():
    # Conexión al broker (RabbitMQ en localhost)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Aseguramos que exista la cola
    channel.exchange_declare(exchange='tareas', exchange_type='direct')

    # Publicamos un mensaje con direct routing
    channel.basic_publish(exchange='tareas',
                        routing_key='seba',  
                        body='Hola, soy Sebastiana Ferriés, que lindo participar de esta clase de ADA!!!')

    print("Mensaje direct enviado.")
    connection.close()

def FanoutPub():
    # Conexión al broker (RabbitMQ en localhost)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Aseguramos que exista la cola
    channel.exchange_declare(exchange='fanout', exchange_type='fanout')

    # Publicamos un mensaje con fanout routing
    channel.basic_publish(exchange='fanout',
                        routing_key='',  # fanout no usa routing key
                        body='Esto es un mensaje de prueba para fanout!!')

    print("Mensaje fanout enviado.")
    connection.close()

def TopicPub():
    # Conexión al broker (RabbitMQ en localhost)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declaramos el exchange topic
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    # Publicamos mensajes con diferentes routing keys para topic routing
    routing_keys = [
        'log.info.user',
        'log.error.database', 
        'log.warning.network',
        'log.debug.application',
        'system.critical.security'
    ]
    
    messages = [
        'Usuario logueado correctamente',
        'Error de conexión a la base de datos',
        'Advertencia: latencia alta en la red',
        'Debug: iniciando proceso de validación',
        'Alerta crítica de seguridad detectada'
    ]

    for routing_key, message in zip(routing_keys, messages):
        channel.basic_publish(exchange='topic_logs',
                            routing_key=routing_key,
                            body=f'[{routing_key}] {message}')
        print(f"Mensaje topic enviado con routing key: {routing_key}")

    connection.close()

DirectPub()
FanoutPub()
TopicPub()