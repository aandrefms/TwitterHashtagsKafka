from kafka.consumer import KafkaConsumer

kafka_boostrap_servers = '127.0.0.1:9092'
kafka_topic_name = 'hashtags'

consumer = KafkaConsumer(kafka_topic_name, bootstrap_servers=kafka_boostrap_servers,
                         auto_offset_reset='earliest', enable_auto_commit=False)
for message in consumer:
    # Fazendo a decodificação das mensagens(estão em bytes)
    print(message.value.decode('utf-8'))