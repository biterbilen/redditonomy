import os
import re
import json
from redditonomy import app
from models import Redis, Results, Session
from flask import request, session, g, redirect, \
    url_for, render_template, send_from_directory, jsonify
from sqlalchemy import distinct

ONE_HOUR = 3600
ONE_DAY = 24 * ONE_HOUR

sess = Session()
cache = Redis()

@app.route('/', methods=['GET'])
def home():
    key = cache.make_key('subreddit_list')
    value = cache.get(key)

    if not value:
        query = sess.query(Results).distinct(Results.subreddit).all()
        subreddits = [q.subreddit for q in query]
        cache.set(key, subreddits, ex=ONE_HOUR)

    return render_template('home.html', subreddits=subreddits)

@app.route('/q/<subreddit>', methods=['GET', 'POST'])
def query(subreddit):
    """
    data looks like this
    results = [{
            'date': '01-{}-2008'.format(i),
            'vocab_size': i,
            'corpus_size': 100,
            'results': [['repub', round(i*0.01, 2)]]*5
        } for i in range(500)]
    return jsonify(results)
    """
    key = cache.make_key(subreddit)
    value = cache.get(key)

    if not value:
        query = sess.query(Results) \
                    .filter(Results.subreddit == subreddit) \
                    .order_by(Results.date) \
                    .all()
        results = [json.loads(str(re.sub('\'', '\"', q.results))) for q in query]
        cache.set(key, results, ex=ONE_DAY)
    return jsonify(results)
