'''
    Front end code to run movie2books.com
    
    Sumin Tang, June 2014
'''

from flask import Flask, render_template, request
import MySQLdb
import requests
from backend import *

app = Flask(__name__)

config_file = './general.mysql'
db = MySQLdb.connect(read_default_file=config_file)

@app.route("/", methods=["GET", "POST"])
def hello():
    return render_template('index.html')

@app.route("/search", methods=["GET", "POST"])
def search():
    keyword = request.form["movietitle"]
    books = recommender(keyword)
    print keyword
    print len(books)
    if len(books)>0:
        # print books[0]
        return render_template('search.html', recbooks = books, keyword = keyword.title())
    else:
        return render_template('error.html')


@app.route("/error", methods=["GET", "POST"])
def errpage():
    return render_template('error.html')

@app.route("/sumin", methods=["GET", "POST"])
def aboutme():
    return render_template('sumin.html')

@app.route("/slides")
def slides():
    return render_template('myslides.html')


@app.route('/<pagename>')
def regularpage(pagename=None):
    """ 
    Route not found by the other routes above. May point to a static template. 
    """
    return "You've arrived at " + pagename


if __name__ == "__main__":
    app.run()