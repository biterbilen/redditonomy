import os
from ConfigParser import ConfigParser
from sqlalchemy import create_engine, Column, String, Integer, Date
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from redditonomy import app
from flask import request, session, g, redirect, \
    url_for, render_template, send_from_directory, jsonify

creds = ConfigParser()
creds.read(os.path.expanduser('~/.aws/credentials'))

user = creds.get('db', 'user')
pwd = creds.get('db', 'password')
ip = creds.get('db', 'ip')
port = creds.get('db', 'port')
name = creds.get('db', 'database')
db = 'postgresql://{user}:{pwd}@{ip}:{port}/{dbname}'.format(
    user=user,
    pwd=pwd,
    ip=ip,
    port=port,
    dbname=name)

engine = create_engine(db, echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Results(Base):
    __tablename__ = 'results'
    extend_existing = True

    id = Column(Integer, primary_key=True)
    subreddit = Column(String)
    date = Column(Date)
    results = Column(postgresql.JSONB())

    def __init__(self, subreddit, date, results):
        self.data = results

Base.metadata.create_all(engine)
sess = Session()

@app.route('/', methods=['GET', 'POST'])
def home():
    query = sess.query(Results).all()

    return render_template('home.html', data=query[0].results)
