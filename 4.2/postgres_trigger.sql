-- Таблица пользователей
drop table if exists users;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица логирования
drop table if exists users_audit;

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- Функция логирования изменений
CREATE OR REPLACE FUNCTION log_user_change()
RETURNS TRIGGER AS $$
DECLARE
    current_user_name TEXT;
BEGIN
    -- Получаем текущего пользователя (можно заменить на другую логику, если используется специальная сессионная переменная)
    SELECT session_user INTO current_user_name;

    -- Если это обновление записи
    IF TG_OP = 'UPDATE' THEN
        -- Проверяем изменение поля name
        IF OLD.name IS DISTINCT FROM NEW.name THEN
            INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
            VALUES (NEW.id, current_user_name, 'name', OLD.name, NEW.name);
        END IF;

        -- Проверяем изменение поля email
        IF OLD.email IS DISTINCT FROM NEW.email THEN
            INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
            VALUES (NEW.id, current_user_name, 'email', OLD.email, NEW.email);
        END IF;

        -- Проверяем изменение поля role
        IF OLD.role IS DISTINCT FROM NEW.role THEN
            INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
            VALUES (NEW.id, current_user_name, 'role', OLD.role, NEW.role);
        END IF;

        -- Обновляем поле updated_at
        NEW.updated_at = CURRENT_TIMESTAMP;

    -- Если это вставка новой записи
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (NEW.id, current_user_name, 'creation', NULL, 'User created');
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Триггер
CREATE TRIGGER trigger_log_user_change
AFTER INSERT OR UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_change();

-- Функция экспорта изменений
CREATE OR REPLACE FUNCTION export_yesterday_audit_to_csv()
RETURNS VOID AS $$
DECLARE
    file_path TEXT;
    yesterday_date TEXT;
BEGIN
    -- Получаем вчерашнюю дату в формате YYYY-MM-DD
    yesterday_date := to_char(CURRENT_DATE - INTERVAL '1 day', 'YYYY-MM-DD');

    -- Формируем путь к файлу
    file_path := '/tmp/users_audit_export_' || yesterday_date || '.csv';

    -- Экспортируем данные
    EXECUTE format('
        COPY (
            SELECT * 
            FROM users_audit 
            WHERE changed_at >= CURRENT_DATE - INTERVAL ''1 day''
              AND changed_at < CURRENT_DATE
        ) TO %L
        WITH DELIMITER '','' CSV HEADER',
        file_path
    );
END;
$$ LANGUAGE plpgsql;

-- Установка расширения
CREATE EXTENSION IF NOT EXISTS cron;

-- Планирование задачи
SELECT cron.schedule(
    '0 3 * * *',  -- Каждый день в 03:00
    $$SELECT export_yesterday_audit_to_csv();$$
);

-- Проверяем заланированные задачи
SELECT * FROM cron.job;

-- Генерируем тестовые данные
INSERT INTO users (name, email, role, updated_at)
VALUES 
('user2', 'u2@example.com', 'user', CURRENT_DATE - INTERVAL '3 day'),
('user3', 'u3@example.com', 'user', CURRENT_DATE - INTERVAL '2 day'),
('moder1', 'm1@example.com', 'moderator', CURRENT_DATE - INTERVAL '3 day'),
('user4', 'u4@example.com', 'user', CURRENT_DATE - INTERVAL '2 day'),
('user5', 'u5@example.com', 'user', CURRENT_DATE - INTERVAL '3 day'),
('moder2', 'm2@example.com', 'moderator', CURRENT_DATE - INTERVAL '2 day'),
('a1', 'a1@example.com', 'author', CURRENT_DATE - INTERVAL '2 day'),
('user6', 'u6@example.com', 'user', CURRENT_DATE - INTERVAL '1 day'),
('moder3', 'm3@example.com', 'moderator', CURRENT_DATE - INTERVAL '1 day'),
('a2', 'a2@example.com', 'author', CURRENT_DATE - INTERVAL '1 day'),
('user7', 'u7@example.com', 'user', CURRENT_DATE - INTERVAL '1 day'),
('a3', 'a3@example.com', 'author', CURRENT_DATE - INTERVAL '1 day'),
('a4', 'a4@example.com', 'author', CURRENT_DATE - INTERVAL '1 day');

-- Правки
Update users set name='user4_1' where name='user4';
Update users set name='user6_1' where name='user6';
Update users set name='user7_1', email='user7@example.com' where name='user6';
Update users set name='a1_1' where name='a1';

-- Изменение даты для тестирования функции экспорта
Update users_audit set changed_at='2025-06-16 21:30:17.019' where id=15;
Update users_audit set changed_at='2025-06-16 21:30:18.352'	where id=16;

-- запуск функции экспорта "вручную"
SELECT export_yesterday_audit_to_csv();