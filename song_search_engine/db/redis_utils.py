__author__ = 'Hari'

import redis


def get_redis_connection(hostname='localhost', port_number=6379,
                         db_number=0, passwd=None):
    """ Function to return redis server connection. """
    pool = redis.ConnectionPool(host=hostname, port=port_number,
                                db=db_number, password=passwd)
    return redis.Redis(connection_pool=pool)
