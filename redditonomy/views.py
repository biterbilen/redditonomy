from redditonomy import app
from flask import request, session, g, redirect, \
    url_for, render_template, send_from_directory, jsonify

@app.route('/')
def home():
    return render_template('def.html')
