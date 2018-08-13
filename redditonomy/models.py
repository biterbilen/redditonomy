import os
import json
import zlib
import hashlib
from ConfigParser import ConfigParser
from sqlalchemy import create_engine, Column, String, Integer, Date
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from redis import StrictRedis, ConnectionError

creds = ConfigParser()
creds.read(os.path.expanduser('~/.aws/credentials'))

user = creds.get('db', 'user')
pwd = creds.get('db', 'password')
ip = creds.get('db', 'ip')
port = creds.get('db', 'port')
name = creds.get('db', 'database')
db = 'postgresql://{user}:{pwd}@{ip}:{port}/{dbname}'.format(
    user=user, pwd=pwd, ip=ip, port=port, dbname=name)

class Redis(StrictRedis):
    REDIS_CREDENTIAL_FILE = os.path.expanduser('~/.aws/credentials')
    redis_config = ConfigParser()
    redis_config.read(REDIS_CREDENTIAL_FILE)

    params = {
        'host': redis_config.get('redis', 'ip'),
        'port': redis_config.get('redis', 'port'),
        'auth': redis_config.get('redis', 'auth')
    }

    def __init__(self):
        super(Redis, self).__init__(host=self.params['host'],
                                    port=self.params['port'],
                                    password=self.params['auth'])

    def make_key(self, subreddit=''):
        m = hashlib.md5()
        m.update(subreddit)
        key = '{hash}'.format(hash=m.hexdigest())
        return key

    def get(self, key):
        try:
            value = super(Redis, self).get(key)
        except ConnectionError:
            value = None
        else:
            if value:
                decompressed = zlib.decompress(value)
                value = json.loads(decompressed)

        return value

    def set(self, key, value, ex=None):
        input = json.dumps(value)
        compressed = zlib.compress(input)

        try:
            super(Redis, self).set(key, compressed, ex=ex)
        except ConnectionError:
            pass

engine = create_engine(db, echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Results(Base):
    __tablename__ = 'newresults'
    extend_existing = True

    id = Column(Integer, primary_key=True)
    subreddit = Column(String)
    date = Column(Date)
    results = Column(String)

    def __init__(self, subreddit, date, results):
        self.data = results

Base.metadata.create_all(engine)
