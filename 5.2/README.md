### Перенос данных из PostgreSQL в Clickhouse с использованием Kafka

#### Использование

1. Перенести файлы в рабочую директорию
1. Создать таблицу исходных данных
1. Заполнить таблицу статовыми значениями
1. Запустить скрипт, собирающий данные в таблицу назначения - consumer.py
1. Запустить скрипт, забирающий данные из исходной таблицы - producer.py

#### PostgreSQL DDL:
CREATE TABLE public.user_logins (
	id serial4 NOT NULL,
	username text NULL,
	event_type text NULL,
	event_time timestamp NULL,
	sent_to_kafka bool DEFAULT true NULL,
	CONSTRAINT user_logins_pkey PRIMARY KEY (id)
);

#### Пример выполнения
1. Исходная таблица данных
![image](/images/5_2_postgres_table_before.png)

2. Целевая таблица
![image](/images/5_2_clickhouse_table_before.png)

3. Запуск consumer.py
![image](/images/consumer_started.png)

4. Запуск producer.py
![image](/images/first_run.png)

5. Остановка и перезапуск producer.py
![image](/images/second_run.png)

6. Остановка и перезапуск consumer.py
![image](/images/consumer_restart.png)

7. Третий запуск producer.py
![image](/images/third_run.png)

8. Исходная таблица после выполнения п.7
![image](/images/5_2_postgres_table_after.png)

9. Целевая таблица после выполнения п.7
![image](/images/5_2_clickhouse_table_after.png)