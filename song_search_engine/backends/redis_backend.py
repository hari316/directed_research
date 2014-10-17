__author__ = 'Hari'

from db import sql_utils
from db import redis_utils
import cPickle


class RedisContext(object):

    ARTIST_CONTEXT_KEY = 'ARTIST_CONTEXT'
    META_CONTEXT_KEY = 'META_CONTEXT'
    LYRICS_CONTEXT_KEY = 'LYRICS_CONTEXT'
    lyrics_path = '/Users/dmd/Documents/dr/mxm_dataset.db'
    song_meta_path = '/Users/dmd/Documents/dr/track_metadata.db'
    artist_path = '/Users/dmd/Documents/dr/artist_similarity.db'

    def __init__(self):
        self.redis_conn = redis_utils.get_redis_connection()
        self.lyrics_conn = sql_utils.get_sql_connection(RedisContext.lyrics_path)
        self.meta_conn = sql_utils.get_sql_connection(RedisContext.song_meta_path)
        self.artist_conn = sql_utils.get_sql_connection(RedisContext.artist_path)
        self.lyrics_key = RedisContext.LYRICS_CONTEXT_KEY
        self.meta_key = RedisContext.META_CONTEXT_KEY
        self.artist_key = RedisContext.ARTIST_CONTEXT_KEY

    def read_lyrics_context(self):
        """Function returns Song Lyrics Context from redis cache."""

        lyrics_context = self.redis_conn.get(self.lyrics_key)
        if lyrics_context:
            print "INFO:: Lyrics retrieved from redis cache."
            return cPickle.loads(lyrics_context)
        else:
            print "INFO:: Lyrics not found in redis cache."
            return self.write_lyrics_context()

    def write_lyrics_context(self):
        """Function stores Song Lyrics Context into redis cache."""

        track_ids = self.__get_all_track_id()
        #meta_info = get_meta_info(meta_conn)
        lyrics_dict = dict()

        for track_id in track_ids:
            tid = str(','.join(track_id))
            #print "Track ID: {0}".format(tid)
            lyrics_dict[tid] = self.__get_song_lyrics(tid)

        if self.redis_conn.set(self.lyrics_key, cPickle.dumps(lyrics_dict)):
            print "INFO:: Lyrics details successfully stored in redis cache."
        else:
            print "ERROR:: Failed to store Lyrics details in redis Cache."
        return lyrics_dict

    def read_meta_context(self):
        """Function returns Song Meta Context from redis cache."""

        meta_context = self.redis_conn.get(self.meta_key)
        if meta_context:
            print "INFO:: Meta data retrieved from redis cache."
            return cPickle.loads(meta_context)
        else:
            print "INFO:: Meta data not found in redis cache."
            return self.write_meta_context()

    def write_meta_context(self):
        """Function stores Song Meta Context into redis cache."""

        track_ids = self.__get_all_track_id()
        #meta_info = get_meta_info(meta_conn)
        meta_dict = dict()

        for track_id in track_ids:
            tid = str(','.join(track_id))
            #print "Track ID: {0}".format(tid)
            meta_dict[tid] = self.__get_meta_info(tid)

        if self.redis_conn.set(self.meta_key, cPickle.dumps(meta_dict)):
            print "INFO:: Meta data details successfully stored in redis cache."
        else:
            print "ERROR:: Failed to store Meta data details in redis Cache."
        return meta_dict

    def read_artist_context(self):
        """Function returns Song Artist Context from redis cache."""

        artist_context = self.redis_conn.get(self.artist_key)
        if artist_context:
            print "INFO:: Artist info retrieved from redis cache."
            return cPickle.loads(artist_context)
        else:
            print "INFO:: Artist info not found in redis cache."
            return self.write_artist_context()

    def write_artist_context(self):
        """Function stores Song Artist Context into redis cache."""

        artist_ids = self.__get_all_artist_id()
        #meta_info = get_meta_info(meta_conn)
        artist_dict = dict()

        for artist_id in artist_ids:
            aid = str(','.join(artist_id).encode('utf-8'))
            #print "Track ID: {0}".format(tid)
            meta = dict()
            meta['similar_artist'] = self.__get_similar_artist(aid)
            meta['similar_track'] = self.__get_track_by_artist(aid)
            artist_dict[aid] = meta

        if self.redis_conn.set(self.artist_key, cPickle.dumps(artist_dict)):
            print "INFO:: Similar Artist details successfully stored in redis cache."
        else:
            print "ERROR:: Failed to store similar artist details in redis Cache."
        return artist_dict

    def flush(self):
        self.redis_conn.delete(self.lyrics_key)
        self.redis_conn.delete(self.meta_key)

    def __get_similar_artist(self, artist_id):
        query = "select similar from similarity where target = '{0}'".format(artist_id.encode('utf-8'))
        result = self.artist_conn.execute(query)
        return result.fetchall()

    def __get_all_artist_id(self):
        query = 'select artist_id from artists'
        result = self.artist_conn.execute(query)
        return result.fetchall()

    def __get_song_lyrics(self, song_id):
        query = "select word from lyrics where track_id = '{0}'".format(song_id)
        result = self.lyrics_conn.execute(query)
        words = result.fetchall()
        lyrics = []
        for word in words:
            lyrics.append(str(','.join(word).encode('utf-8')))
        return lyrics

    def __get_all_track_id(self):
        query = 'select track_id from songs'
        result = self.meta_conn.execute(query)
        return result.fetchall()

    def __get_meta_info(self, song_id):
        query = "select track_id, title, artist_id, artist_name, year from songs where track_id = '{0}'".format(song_id)
        meta_dict = dict()
        try:
            result = self.meta_conn.execute(query)
            result_parsed = result.fetchall()
            r = result_parsed[0]
            meta_dict['title'] = str(r[1].encode('utf-8'))
            meta_dict['artist_id'] = str(r[2].encode('utf-8'))
            meta_dict['artist_name'] = str(r[3].encode('utf-8'))
            meta_dict['year'] = str(r[4])
        except Exception, e:
            print "ERROR: " + e

        return meta_dict

    def __get_track_by_artist(self, artist_id):
        query = "select track_id from songs where artist_id = '{0}'".format(artist_id)
        result = self.meta_conn.execute(query)
        return result.fetchall()



