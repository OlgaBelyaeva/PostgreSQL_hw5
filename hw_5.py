# Домашнее задание к лекции «Работа с PostgreSQL из Python»

import psycopg2

def drop_table():
    cur.execute('''
    DROP TABLE phone;
    DROP TABLE client;        
    ''')
    conn.commit()
    print('Таблицы удалены')

def create_table(conn):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS client(
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(30) NOT NULL,
        surname VARCHAR(30) NOT NULL,
        email VARCHAR(30) NOT NULL
        );
    CREATE TABLE IF NOT EXISTS phone(
        phone_id SERIAL PRIMARY KEY,
        client_id INTEGER NOT NULL REFERENCES client(client_id),
        phone_number VARCHAR(10)
        );
    ''')
    conn.commit()
    print('Таблицы созданы')

def new_client(conn, name, surname, email, phone_number=None):
    cur.execute('''
    INSERT INTO client (name, surname, email) 
    VALUES (%s, %s, %s)
    RETURNING client_id;
    ''', (name, surname, email))
    client_id = cur.fetchone()
    print('Добавлен новый клиент c id ', client_id)
    if phone_number != None:
        cur.execute('''
        INSERT INTO phone (client_id, phone_number)
        VALUES (%s, %s);
        ''', (client_id, phone_number))
        conn.commit()

def add_phone(conn, client_id, phone_number):
    cur.execute('''
    SELECT phone_number FROM phone 
    WHERE client_id = %s AND phone_number = %s;
    ''', (client_id, phone_number))
    if cur.fetchone() != None:
        print('Такой номер у клиента уже есть')
    else:
        cur.execute('''
        INSERT INTO phone (client_id, phone_number)
        VALUES (%s, %s)
        RETURNING client_id, phone_number;
        ''', (client_id, phone_number))
        print('Телефон добавлен', cur.fetchone())

def change_client(conn, client_id, name=None, surname=None, email=None, new_phone_number=None):
    if name != None:
        cur.execute('''
        UPDATE client
        SET name = %s
        WHERE client_id = %s
        RETURNING client_id, name;
        ''', (name, client_id))
        print('Имя клиента изменено: ', cur.fetchone())
    if surname != None:
        cur.execute('''
        UPDATE client
        SET surname = %s
        WHERE client_id = %s
        RETURNING client_id, surname;
        ''', (surname, client_id))
        print('Фамилия клиента изменена: ', cur.fetchone())
    if email != None:
        cur.execute('''
        UPDATE client
        SET email = %s
        WHERE client_id = %s
        RETURNING client_id, name;
        ''', (email, client_id))
        print('Почта клиента изменена: ', cur.fetchone())
    if new_phone_number != None:
        cur.execute('''
        SELECT phone_number FROM phone
        WHERE client_id = %s;
        ''', (client_id,))
        client_phones = cur.fetchall()
        if len(client_phones) == 0:
            print('У данного клиента нет телефонных номеров, изменять нечего. Используйте функцию внесения номера.')
        elif len(client_phones) == 1:
            cur.execute('''
            UPDATE phone
            SET phone_number = %s
            WHERE client_id = %s
            RETURNING client_id, phone_number; 
            ''', (new_phone_number, client_id))
            print('Телефон клиента изменен: ', cur.fetchone())
        elif len(client_phones) > 1:
            print('За данным клиентом зкреплены несколько телефонных номеров: ', client_phones)
            phone_number = input('Введите номер, который надо заменить: ')
            cur.execute('''
            UPDATE phone
            SET phone_number = %s
            WHERE client_id = %s AND phone_number = %s
            RETURNING client_id, phone_number; 
            ''', (new_phone_number, client_id, phone_number))
            print('Телефон клиента изменен: ', cur.fetchone())

def delete_phone(conn, client_id, phone_number):
    cur.execute('''
    DELETE FROM phone
    WHERE client_id = %s AND phone_number = %s
    ''', (client_id, phone_number))
    print(f'Номер телефона {phone_number} у клиента с id = {client_id} удален')

def delete_client(conn, client_id):
    cur.execute('''
    DELETE FROM phone
    WHERE client_id = %s
    ''', (client_id,))
    cur.execute('''
    DELETE FROM client
    WHERE client_id = %s
    ''', (client_id,))
    print(f'Клиент с идентификатором {client_id} удалён')

def search_client(conn, name=None, surname=None, email=None, phone_number=None):
    if name != None and surname == None:
        cur.execute('''
        SELECT * FROM client 
        WHERE name = %s;
        ''', (name,))
        print(f'Клиенты с именем {name}: {cur.fetchall()}')
    if surname != None and name == None:
        cur.execute('''
        SELECT * FROM client 
        WHERE surname = %s;
        ''', (surname,))
        print(f'Клиенты с фамилией {surname}: {cur.fetchall()}')
    if name != None and surname != None:
        cur.execute('''
        SELECT * FROM client 
        WHERE name = %s AND surname = %s;
        ''', (name, surname))
        print(f'Клиенты с именем {name} и фамилией {surname}: {cur.fetchall()}')
    if email != None:
        cur.execute('''
        SELECT * FROM client 
        WHERE email = %s;
        ''', (email,))
        print(f'Клиенты с почтой {email}: {cur.fetchall()}')
    if phone_number != None:
        cur.execute('''
        SELECT c.client_id, name, surname, phone_number
        FROM client c
        FULL JOIN phone p ON c.client_id = p.client_id 
        WHERE phone_number = %s;
        ''', (phone_number,))
        print(f'Клиенты с телефонным номером {phone_number}: {cur.fetchall()}')

with psycopg2.connect(database='client_db', user='postgres', password='Odnerka1') as conn:
    with conn.cursor() as cur:

        # drop_table()
        create_table(conn)
        new_client(conn, 'Андрей', 'Андреев', 'a@mail', '1111111111')
        new_client(conn, 'Борис', 'Борисов', 'b@mail')
        new_client(conn, 'Виктор', 'Викторов', 'v@mail', '3333333333')
        new_client(conn, 'Григорий', 'Григорьев', 'g@mail', '4444444444')
        new_client(conn, 'Дмитрий', 'Дмитриев', 'd@mail', '3333333333')
        new_client(conn, 'Евгений', 'Борисов', 'e@mail')
        add_phone(conn, 1, '1111111111')
        add_phone(conn, 1, '5555555555')
        add_phone(conn, 1, '3333333333')
        change_client(conn, client_id=1, name='Дрон', surname='Дронов', new_phone_number='6666666666')
        change_client(conn, client_id=2, new_phone_number='7777777777')
        change_client(conn, client_id=4, new_phone_number='7777777777')
        delete_phone(conn, client_id='1', phone_number='1111111111')
        delete_client(conn, client_id='4')
        search_client(conn, name='Дрон')
        search_client(conn, surname='Борисов')
        search_client(conn, name='Виктор', surname='Викторов')
        search_client(conn, email='a@mail')
        search_client(conn, phone_number='3333333333')

conn.close
