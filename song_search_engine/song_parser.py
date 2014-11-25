__author__ = 'Hari'
import json
import time
from backends import redis_backend
#from flask import Flask
from flask import Flask, request

app = Flask(__name__)
#api = restful.Api(app)

@app.route("/")
def test_running():
    return "INFO:: Server is running ...."

@app.route("/hello")
def hello_world(name='Hari'):
    return "Hello %s !!, How are you ?" % name

@app.route("/song_lyrics/<song_id>")
def get_song_lyrics(song_id):
    start_time = time.time()
    print "SONG ID: "+song_id

    if song_id:
        lyrics = parser.read_lyrics_context()
        print " ------- EXECUTION TIME ELAPSED :: {0} SECONDS -------".format(time.time() - start_time)
        return str(lyrics[song_id])
    else:
        return "INVALID SONG_ID"

#@app.route("/song_info/<song_id>")
def get_song_info(song_id):
    start_time = time.time()
    print "SONG ID: " + song_id
    result = dict()
    if song_id:
        meta_info = parser.read_meta_context()
        similar = parser.read_artist_context()

        song_details = meta_info[song_id]
        similar_info = similar[song_details['artist_id']]
        result['meta_info'] = song_details

        print "ARTIST ID: " + song_details['artist_id']

        result['similar_artist'] = similar_info['similar_artist']
        result['similar_track'] = similar_info['similar_track']

        print result.keys()
       #print " ------- EXECUTION TIME ELAPSED :: {0} SECONDS -------".format(time.time() - start_time)
        return str(result)
    else:
        return "INVALID SONG_ID"


@app.route("/artist_songs/<artist_id>")
def get_artist_songs(artist_id):
    start_time = time.time()
    print "ARTIST ID: " + artist_id
    result = dict()
    if artist_id:
        similar = parser.read_artist_context()
        similar_info = similar[artist_id]
        #result['similar_artist'] = similar_info['similar_artist']
        result['tracks'] = similar_info['similar_track']

        print result.keys()
        print " ------- EXECUTION TIME ELAPSED :: {0} SECONDS -------".format(time.time() - start_time)
        return str(result)
    else:
        return "INVALID SONG_ID"


@app.route("/all_song_lyrics/")
def get_all_song_lyrics():
    start_time = time.time()
    lyrics = parser.read_lyrics_context()
    print " ------- EXECUTION TIME ELAPSED :: {0} SECONDS -------".format(time.time() - start_time)
    return json.dumps(lyrics)
    #return str(lyrics)

@app.route("/song_info", methods=['GET', 'POST'])
def get_all_song_details():
    start_time = time.time()
    song_ids = request.get_json(force=True)
    result = {}
    if 'ids' in song_ids:
	for id in song_ids['ids']:
	    result[id] = get_song_info(id)

    print " ------- EXECUTION TIME ELAPSED :: {0} SECONDS -------".format(time.time() - start_time)
    return json.dumps(result)

parser = redis_backend.RedisContext()

if __name__ == '__main__':

    app.run(host='0.0.0.0')
    print "SONG REST SERVICE IS RUNNING .... "

