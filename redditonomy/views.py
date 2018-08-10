import os
import json
from redditonomy import app
from models import Redis, Results, Session
from flask import request, session, g, redirect, \
    url_for, render_template, send_from_directory, jsonify

ONE_DAY = 24 * 3600

sess = Session()
cache = Redis()

@app.route('/', methods=['GET'])
def home():
    #query = sess.query(Results).all()
    subreddits = ['AskReddit', 'worldnews', 'nba']
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
    query = sess.query(Results).filter(Results.subreddit == subreddit).all()

    key = cache.make_key(subreddit)
    value = cache.get(key)

    if not value:
        results = [q.results for q in query]
        cache.set(key, results, ex=ONE_DAY)
    raise
    return jsonify(results)
