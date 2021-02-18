import sqlalchemy
from pprint import pprint
import requests
import time
import os
import json


def get_token(file_name: str):
    """получить токен из файла
    временный токен можно получить по адресу https://developer.spotify.com/console/"""
    with open(file_name, 'r', encoding='utf-8') as f:
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
    time.sleep(0.5)
    if response.json()['artists']['items']:
        author = {'id': response.json()['artists']['items'][0]['id'],
                  'name': response.json()['artists']['items'][0]['name'],
                  'genre': response.json()['artists']['items'][0]['genres']
                  }
        if author['name'].lower() == name.lower():
            output = author

    else:
        output = {}

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
        albums.append({'id': entry['id'], 'year': entry["release_date"].split('-')[0], 'name': entry["name"]})

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
        tracks.append({'id': entry['id'], 'duration': int(int(entry["duration_ms"]) / 1000),
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


def insert_it_in_db(it):
    passw = 'myspotifypass'
    db = f'postgresql://myspotify:{passw}@localhost:5432/myspotify'
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()

    # для тестов - очищаю всёб что было
    clear_db(connection)
    # очистка лога
    del_log()

    this_author_id = insert_author(it, connection)
    print(this_author_id)
    its_genre_ids = insert_genre(it, connection)
    print(its_genre_ids )


def insert_genre(it, connection):
    genre_ids = []
    for entry in it['genre']:
        result = connection.execute(f"""SELECT id FROM genre WHERE name = '{entry}';""").fetchone()
        if not result:
            query_string = f"""INSERT INTO genre(name)
                    VALUES('{entry}');"""
            connection.execute(query_string)
            log_it(query_string)
            one_genre_id = get_max_id('genre', connection)
        else:
            one_genre_id = result[0]

        genre_ids.append(one_genre_id)

    return genre_ids


    # result = connection.execute("""SELECT * FROM author;""").fetchmany(10)
    # pprint(result)
    #
    # result = connection.execute("""SELECT first_name, last_name FROM actor
    # WHERE last_name LIKE '%%ge%%' ORDER BY first_name;""").fetchall()
    # pprint(result)


def insert_author(it, connection):
    query_string = f"""INSERT INTO author(name)
           VALUES('{it["name"]}');"""
    connection.execute(query_string)
    log_it(query_string)
    author_id = get_max_id('author', connection)
    print(author_id)
    return author_id


def get_max_id(table_name: str, connection):
    query_string = f"""SELECT max(id) FROM {table_name};"""
    max_id = connection.execute(query_string).fetchone()[0]
    return max_id


def log_it(entry: str):
    with open('query_log.txt', 'a') as f:
        f.write(entry + '\n')


def del_log():
    if 'query_log.txt' in os.listdir():
        path = os.path.join('query_log.txt')
        os.remove(path)


def clear_db(connection):
    delete_list = (
        'author', 'genre', 'album', 'track', 'compilation', 'Genre_Author', 'Album_Author', 'Compilation_Track')
    for entry in delete_list:
        query_string = f"""DELETE FROM {entry};"""
        connection.execute(query_string)


def gogo():
    it = get_all_about('them crooked vultures')
    if it:
        insert_it_in_db(it)


if __name__ == '__main__':
    gogo()
