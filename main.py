import sqlalchemy
import requests
import time
import os


BAND_LIST = ['Iron Maiden',
             'Nirvana',
             'Pearl Jam',
             'Pink Floyd',
             'Led Zeppelin',
             'Radiohead',
             'U2',
             'Depeche Mode']

DB_USER = 'myspotify'
DATABASE = 'myspotify'


def get_token(file_name: str):
    """получить токен из файла
    временный токен можно получить по адресу https://developer.spotify.com/console/"""
    with open(file_name, 'r', encoding='UTF-8') as f:
        token = f.readline()
    return token


def get_author(name: str):
    """Поиск автора по именни"""
    token = get_token('spotify_token.txt')
    endpoint = 'https://api.spotify.com/v1/search'
    response = requests.get(endpoint,
                            headers={"Accept": "application/json",
                                     "Content-Type": "application/json",
                                     "Authorization": "Bearer " + token},
                            params={'q': name, 'type': 'artist'})
    response.raise_for_status()
    time.sleep(2.5)
    output = {}
    if response.json()['artists']['items']:
        author = {'id': response.json()['artists']['items'][0]['id'],
                  'name': [response.json()['artists']['items'][0]['name']],
                  'genre': response.json()['artists']['items'][0]['genres']
                  }
        if author['name'][0].lower() == name.lower():
            output = author
        else:
            print(f'Не найдено точного совпадения имени ртиста {name}')

    return output


def get_albums(id_: str):
    """Список альбомов автора по id автора"""
    token = get_token('spotify_token.txt')
    endpoint = f'https://api.spotify.com/v1/artists/{id_}/albums'
    response = requests.get(endpoint,
                            headers={"Accept": "application/json",
                                     "Content-Type": "application/json",
                                     "Authorization": "Bearer " + token})
    response.raise_for_status()
    time.sleep(0.5)

    albums = []
    for entry in response.json()['items']:
        albums.append({'id': entry['id'],
                       'year': entry["release_date"].split('-')[0],
                       'name': entry["name"]})

    return albums


def get_tracks(id_: str):
    """Список треков по id альбома"""
    token = get_token('spotify_token.txt')
    endpoint = f'https://api.spotify.com/v1/albums/{id_}/tracks'
    response = requests.get(endpoint,
                            headers={"Accept": "application/json",
                                     "Content-Type": "application/json",
                                     "Authorization": "Bearer " + token})
    response.raise_for_status()
    time.sleep(0.5)

    tracks = []
    for entry in response.json()['items']:
        tracks.append({'id': entry['id'],
                       'duration': int(int(entry["duration_ms"]) / 1000),
                       'name': entry["name"]})

    return tracks


def get_all_about(name: str):
    """формирует структуру со автором альбомом и треками для дальнейшего помещения в базу"""
    author = get_author(name)
    output = author
    if author:
        albums = get_albums(author['id'])
        output['albums'] = albums

        for entry in output['albums']:
            print(entry['name'])
            tracks = get_tracks(entry['id'])
            entry['tracks'] = tracks

    return output


def get_connection(passw: str):
    """логинится в базу"""
    db = f'postgresql://{DB_USER}:{passw}@localhost:5432/{DATABASE}'
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()
    return connection


def put_author(it, connection, log_file_name=None):
    """Помещает автора в базу"""
    author_id = [insert_fields(f"""'{no_b(it['name'][0])}'""",
                               'author', 'name', connection, log_file_name)]
    print(f'добавлен исполнитель {it["name"][0]}')
    return author_id


def put_genre(it, connection, log_file_name=None):
    """Помещает список жанров в базу"""
    genre_ids = []
    for entry in it['genre']:
        result = is_it_there(f"""name = '{no_b(entry)}'""", 'genre', connection)
        if result:  # уже есть запись с таким значением, а должна быть уникальной
            one_item_id = result[0]
        else:
            one_item_id = insert_fields(f"""'{no_b(entry)}'""",
                                        "genre", "name", connection, log_file_name)
            print(f'добавлен жанр {entry}')

        genre_ids.append(one_item_id)

    return genre_ids


def put_albums(it, connection, log_file_name=None):
    """Помещает альбомы и треки в базу"""
    album_ids = []
    for entry in it['albums']:
        album_id = insert_fields(f"""'{no_b(entry["name"])}', {entry['year']}""",
                                 "album", "name, year", connection, log_file_name)
        print(f'добавлен альбом {entry["name"]}')
        track_ids = []
        for num, item in enumerate(entry['tracks']):
            one_track_id = insert_fields(f""" '{no_b(item['name'])}', {item['duration']}, {album_id}""",
                                         "track", "name, duration, album_id", connection, log_file_name)
            print(f'трек {num} {item["name"]}')
            track_ids.append(one_track_id)

        album_ids.append({'album_id': [album_id], 'track_ids': track_ids})

    return album_ids


def bind_it(ids_1: list, ids_2: list, table: str, column_1: str, column_2: str, connection, log_file_name=None):
    """связывает два списка id"""
    for id_1 in ids_1:
        for id_2 in ids_2:
            what = f"""{column_1} = {id_1} and {column_2} = {id_2}"""
            result = connection.execute(f"""SELECT * FROM {table} WHERE {what};""").fetchone()
            if not result:  # запись с таким значением должна быть уникальной
                query_string = f"""INSERT INTO {table}({column_1}, {column_2})\nVALUES({id_1}, {id_2});"""
                connection.execute(query_string)
                if log_file_name:
                    log_it(query_string, log_file_name)


def insert_it_in_db(it, connection, log_file_name=None):
    """помещает в базу структуру одного испонителя со всем его содержимым"""
    author_id = put_author(it, connection, log_file_name)
    genre_ids = put_genre(it, connection, log_file_name)
    album_ids = put_albums(it, connection, log_file_name)
    bind_it(genre_ids, author_id, 'genre_author', 'genre_id', 'author_id', connection, log_file_name)
    for entry in album_ids:
        bind_it(entry['album_id'], author_id, 'album_author', 'album_id', 'author_id', connection, log_file_name)


def insert_fields(what: str, table: str, fields: str, connection, log_file_name=None):
    """заполняет нужную строчку в нужной таблице базы"""
    query_string = f"""INSERT INTO {table}({fields})\nVALUES({what});""".encode('UTF-8').decode('1251', errors='ignore')
    connection.execute(query_string)  # упарился тут с нетрадиционными символами
    if log_file_name:
        log_it(query_string, log_file_name)

    item_id = get_max_id(table, connection)
    return item_id


def s_q(query_string: str, connection, log_file_name=None):
    """делает запрос в базу"""
    result = connection.execute(query_string).fetchall()
    if log_file_name:
        log_it(query_string, log_file_name)

    return result


def is_it_there(what: str, table: str, connection):
    """проверяет, есть ли в базу такой элемент"""
    result = connection.execute(f"""SELECT id FROM {table} WHERE {what};""").fetchone()
    return result


def get_max_id(table_name: str, connection):
    """Ищет последний id"""
    query_string = f"""SELECT max(id) FROM {table_name};"""
    max_id = connection.execute(query_string).fetchone()[0]
    return max_id


def log_it(entry: str, log_file_name: str):
    """помещает строку в лог файл"""
    with open(log_file_name, 'a', encoding='UTF-8') as f:
        f.write(entry)
        f.write('\n\n')


def del_log(log_file_name: str):
    """удаляет файл лога"""
    if log_file_name in os.listdir():
        path = os.path.join(log_file_name)
        os.remove(path)


def clear_db(connection):
    """очищает базу (нужно для тестирования)"""
    delete_list = ('Genre_Author', 'Album_Author', 'Compilation_Track', 'track',
                   'author', 'genre', 'album', 'compilation',)
    for entry in delete_list:
        query_string = f"""DELETE FROM {entry};"""
        connection.execute(query_string)


def no_b(name: str):
    """меняет в строке кавычки на две кавычки, чтобы SQL запрос выполнился верно"""
    no_brackets = name.replace('\'', '\'\'')
    return no_brackets


def put_compilation(name: str, year: int, query_string, connection, log_file_name=None):
    """заполняет таблицу зборника и связи"""
    result = s_q(query_string, connection, log_file_name)
    track_ids = []
    for entry in result:
        track_ids.append(entry[0])

    compilation_id = [insert_fields(f""" '{name}', {year} """, 'compilation', 'name, year', connection, log_file_name)]
    bind_it(compilation_id, track_ids, 'compilation_track ', 'compilation_id', 'track_id', connection, log_file_name)


def make_some_compilations(connection, log_file_name):
    """Создаёт не менее восьми сборников по разным запросам"""
    query_string = f"""SELECT id FROM track ORDER BY duration ASC LIMIT 20;"""
    name = 'Shortest Songs'
    year = 2015
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track ORDER BY duration DESC LIMIT 20;"""
    name = 'Longest Songs'
    year = 2016
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track WHERE duration BETWEEN 200 AND 400 LIMIT 20;"""
    name = 'Some Middle Songs'
    year = 2017
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track WHERE name LIKE '%% My %%' LIMIT 20;"""
    name = 'My Songs'
    year = 2018
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track WHERE name LIKE '%% Love %%' LIMIT 20;"""
    name = 'Love Songs'
    year = 2019
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track WHERE name LIKE '%% Song %%' LIMIT 20;"""
    name = 'Song Songs'
    year = 2019
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track WHERE name NOT LIKE '%% %%' LIMIT 20;"""
    name = 'Short Name Songs'
    year = 2020
    put_compilation(name, year, query_string, connection, log_file_name)

    query_string = f"""SELECT id FROM track WHERE (duration %% 100 = 0) LIMIT 10;"""
    name = 'Hundred Songs'
    year = 2021
    put_compilation(name, year, query_string, connection, log_file_name)
    # с последним запросом проблеммка со знаком %, пишлось в логе руками поправить


def hw_4_2():
    """запросы для второго задания"""
    log_file_name = 'select_log.txt'
    del_log(log_file_name)

    password = get_token('passw.txt')
    connection = get_connection(password)

    query_string = f"""SELECT name, year FROM album WHERE year = 2019;"""
    s_q(query_string, connection, log_file_name)

    query_string = f"""SELECT name, duration FROM track ORDER BY duration DESC LIMIT 1;"""
    s_q(query_string, connection, log_file_name)

    query_string = f"""SELECT name FROM track WHERE duration > 210 ORDER BY duration ASC;"""
    s_q(query_string, connection, log_file_name)

    query_string = f"""SELECT name FROM compilation WHERE year BETWEEN 2018 AND 2020;"""
    s_q(query_string, connection, log_file_name)

    query_string = f"""SELECT name FROM author WHERE name NOT LIKE '%% %%';"""
    s_q(query_string, connection, log_file_name)

    query_string = f"""SELECT name FROM track WHERE name LIKE '%% My %%' or name LIKE '%% мой %%';"""
    s_q(query_string, connection, log_file_name)


def gogo():
    """наполняе базу данных"""
    password = get_token('passw.txt')
    connection = get_connection(password)
    log_file_name = 'query_log.txt'

    if False:
        # для тестов - очищаю всё, что было
        clear_db(connection)
        # очистка лога
        del_log(log_file_name)

    for entry in BAND_LIST:
        it = get_all_about(entry)
        if it:
            insert_it_in_db(it, connection, log_file_name)

    make_some_compilations(connection, log_file_name)
    print('DONE!')


if __name__ == '__main__':
    gogo()
    hw_4_2()
