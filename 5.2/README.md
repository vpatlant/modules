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
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/5_2_postgres_table_before.png?raw=true)

2. Целевая таблица
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/5_2_clickhouse_table_before.png?raw=true)

3. Запуск consumer.py
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/consumer_started.png?raw=true)

4. Запуск producer.py
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/first_run.png?raw=true)

5. Остановка и перезапуск producer.py
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/second_run.png?raw=true)

6. Остановка и перезапуск consumer.py
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/consumer_restart.png?raw=true)

7. Третий запуск producer.py
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/third_run.png?raw=true)

8. Исходная таблица после выполнения п.7
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/5_2_postgres_table_after.png?raw=true)

9. Целевая таблица после выполнения п.7
![image](https://github.com/vpatlant/modules/tree/main/5.2/images/5_2_clickhouse_table_after.png?raw=true)
