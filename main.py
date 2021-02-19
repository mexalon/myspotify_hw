import sqlalchemy
from pprint import pprint
import requests
import time
import os
import json

BAND_LIST = ['All Them Witches',
             'Black Peaks',
             'Cynic',
             'Disillusion',
             'King Buffalo',
             'Artificial Language',
             'Haken',
             'Leprous']


def get_token(file_name: str):
    """получить токен из файла
    временный токен можно получить по адресу https://developer.spotify.com/console/"""
    with open(file_name, 'r', encoding='UTF-8') as f:
        token = f.readline()
    return token


def get_author(name: str):
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
    db = f'postgresql://myspotify:{passw}@localhost:5432/myspotify'
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()
    return connection


def put_author(it, connection):
    author_id = [insert_fields(f"""'{no_b(it['name'][0])}'""", 'author', 'name', connection)]
    print(f'добавлен исполнитель {it["name"][0]}')
    return author_id


def put_genre(it, connection):
    genre_ids = []
    for entry in it['genre']:
        result = is_it_there(f"""name = '{no_b(entry)}'""", 'genre', connection)
        if result:  # уже есть запись с таким значением, а должна быть уникальной
            one_item_id = result[0]
        else:
            one_item_id = insert_fields(f"""'{no_b(entry)}'""", "genre", "name", connection)
            print(f'добавлен жанр {entry}')

        genre_ids.append(one_item_id)

    return genre_ids


def put_albums(it, connection):
    album_ids = []
    for entry in it['albums']:
        album_id = insert_fields(f"""'{no_b(entry["name"])}', {entry['year']}""", "album", "name, year", connection)
        print(f'добавлен альбом {entry["name"]}')
        track_ids = []
        for num, item in enumerate(entry['tracks']):
            one_track_id = insert_fields(f""" '{no_b(item['name'])}', {item['duration']}, {album_id}""",
                                         "track", "name, duration, album_id", connection)
            print(f'трек {num} {item["name"]}')
            track_ids.append(one_track_id)

        album_ids.append({'album_id': [album_id], 'track_ids': track_ids})

    return album_ids


def bind_it(ids_1: list, ids_2: list, table: str, column_1: str, column_2: str, connection):
    for id_1 in ids_1:
        for id_2 in ids_2:
            what = f"""{column_1} = {id_1} and {column_2} = {id_2}"""
            result = connection.execute(f"""SELECT * FROM {table} WHERE {what};""").fetchone()
            if not result:  # запись с таким значением должна быть уникальной
                query_string = f"""INSERT INTO {table}({column_1}, {column_2})\nVALUES({id_1}, {id_2});"""
                connection.execute(query_string)
                log_it(query_string)


def insert_it_in_db(it, connection):
    author_id = put_author(it, connection)
    genre_ids = put_genre(it, connection)
    album_ids = put_albums(it, connection)
    bind_it(genre_ids, author_id, 'genre_author', 'genre_id', 'author_id', connection)
    for entry in album_ids:
        bind_it(entry['album_id'], author_id, 'album_author', 'album_id', 'author_id', connection)


def insert_fields(what: str, table: str, fields: str, connection):
    query_string = f"""INSERT INTO {table}({fields})\nVALUES({what});""".encode('UTF-8').decode('1251', errors='replace')
    connection.execute(query_string)  # упарился тут с нетрадиционными символами
    log_it(query_string)
    item_id = get_max_id(table, connection)
    return item_id


def is_it_there(what: str, table: str, connection):
    result = connection.execute(f"""SELECT id FROM {table} WHERE {what};""").fetchone()
    return result


def get_max_id(table_name: str, connection):
    query_string = f"""SELECT max(id) FROM {table_name};"""
    max_id = connection.execute(query_string).fetchone()[0]
    return max_id


def log_it(entry: str):
    with open('query_log.txt', 'a', encoding='UTF-8') as f:
        f.write(entry)
        f.write('\n\n')


def del_log():
    if 'query_log.txt' in os.listdir():
        path = os.path.join('query_log.txt')
        os.remove(path)


def clear_db(connection):
    delete_list = ('Genre_Author', 'Album_Author', 'Compilation_Track', 'track',
                   'author', 'genre', 'album', 'compilation',)
    for entry in delete_list:
        query_string = f"""DELETE FROM {entry};"""
        connection.execute(query_string)


def no_b(name: str):
    no_brackets = name.replace('\'', '\'\'')
    return no_brackets


def gogo():
    password = get_token('passw.txt')
    connection = get_connection(password)

    # для тестов - очищаю всёб что было
    clear_db(connection)
    # очистка лога
    del_log()

    # it = get_all_about('')
    for entry in BAND_LIST:
        it = get_all_about(entry)
        if it:
            insert_it_in_db(it, connection)


if __name__ == '__main__':
    gogo()
