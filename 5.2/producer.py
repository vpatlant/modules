import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute(
    """    
        select 
            case 
                when sent_to_kafka=true then 'to_sent'
                else 'sended'
            end,
        count(*)
        FROM user_logins
        group by sent_to_kafka
    """
)

stat = cursor.fetchall()

sended = 0
to_sent = 0

for el in stat:
    exec(f'{el[0]} = {el[1]}')

print('Отправлено:',sended, '\nК отправке:', to_sent)
cursor.execute("SELECT id, username, event_type, extract(epoch FROM event_time) FROM user_logins where sent_to_kafka=True")
rows = cursor.fetchall()

for row in rows:
    r_id = row[0]
    data = {
        "user": row[1],
        "event": row[2],
        "timestamp": float(row[3])  # преобразуем Decimal → float
    }

    try:
        producer.send("user_events_5_2", value=data)
        print("Sent:", data)
        
        cursor.execute(
        f"""
        UPDATE user_logins
        SET sent_to_kafka = FALSE
        WHERE id = {r_id};
        """    
        )
        
        conn.commit()
        
    except:
        pass
    time.sleep(0.5)
