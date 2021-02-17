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
    time.sleep(0.5)

    author = {'id': response.json()['artists']['items'][0]['id'],
              'name': response.json()['artists']['items'][0]['name'],
              'genre': response.json()['artists']['items'][0]['genres']
              }
    return author


def get_albums(id_: str):
    token = get_token('spotify_token.txt')
    endpoint = f'https://api.spotify.com/v1/artists/{id_}/albums'
    response = requests.get(endpoint,
                            headers={"Accept": "application/json",
                                     "Content-Type": "application/json",
                                     "Authorization": "Bearer " + token})
    time.sleep(0.5)

    albums = []
    for entry in response.json()['items']:
        albums.append({'id': entry['id'],  'year': entry["release_date"].split('-')[0], 'name': entry["name"]})

    return albums


def get_tracks(id_: str):
    token = get_token('spotify_token.txt')
    endpoint = f'https://api.spotify.com/v1/albums/{id_}/tracks'
    response = requests.get(endpoint,
                            headers={"Accept": "application/json",
                                     "Content-Type": "application/json",
                                     "Authorization": "Bearer " + token})
    time.sleep(0.5)

    tracks = []
    for entry in response.json()['items']:
        tracks.append({'id': entry['id'], 'duration': int(int(entry["duration_ms"])/1000),
                       'name': entry["name"]})

    return tracks


def get_all_about(name: str):
    author = get_author('Pearl Jam')
    output = author

    albums = get_albums(author['id'])
    output['albums'] = albums

    for entry in output['albums']:
        print(entry['name'])
        tracks = get_tracks(entry['id'])
        entry['tracks'] = tracks

    pprint(output)


if __name__ == '__main__':

    get_all_about('Peral Jam')

    passw = 'myspotifypass'
    db = f'postgresql://myspotify:{passw}@localhost:5432/myspotify'
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()

    result = connection.execute("""SELECT * FROM author;""").fetchmany(10)
    pprint(result)
    #
    # result = connection.execute("""SELECT first_name, last_name FROM actor
    # WHERE last_name LIKE '%%ge%%' ORDER BY first_name;""").fetchall()
    # pprint(result)
