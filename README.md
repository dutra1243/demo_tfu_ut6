# have python installed
pip install pika

# have docker installed
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
