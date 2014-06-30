"""
    Calculate similarity scores between movies and books using description and review text.
    
    Inputs:
    1507 Books_clean3_unique.txt ~60MB
    2884 Movies_clean3_unique.txt ~110MB
    Columns: product_id, n_review, avgrating, title, description, reviews
    (Note that some of the description is empty)
    
    ID files
    1507 Books_clean3_id.csv
    2884 Movies_clean3_id.csv
    
    Output:
    item1_id, item2_id, similarityscore

    Sumin Tang, June 17, 2014
"""

import nltk
from nltk import word_tokenize   
from nltk import WordNetLemmatizer
import sklearn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import scipy as scipy
import numpy as np
import pickle
import time
import MySQLdb
import re
from snap_utils import *

tic = time.time()

# load the book/movie id data
idtable = 'MoviesBooks3_id'
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

cur.execute('SELECT key_id, product_id from %s' % idtable)
results = np.array(cur.fetchall())
keyids = np.array(results[:, 0], int)
productids = np.array(results[:, 1], str)
db.close()


# ===== load the reviewfile
datadir = './data/'
reviewfile1 = "Movies_clean3_unique.txt"
reviewfile2 = "Books_clean3_unique.txt"

kid = [] # key_id
pid = [] # product_id
nrev = [] # number of reviews
avgrt = [] # average rating
text = [] # text: title + description + reviews

# load movies
f = open(datadir + reviewfile1, 'r')
for l in f:
    l = l.strip()
    values = l.split("::", 3) 
    myid = values[0]
    mytext = values[3] # title + description + reviews
        
    pid.append(myid)
    text.append(mytext)
    nrev.append(values[1])
    avgrt.append(values[2])
        
    # get the key_id
    ix = np.where(productids == myid)[0]
    if len(ix)>0:
        kid.append(keyids[ix[0]])
    else:
        kid.append(0)

# load Books
f = open(datadir + reviewfile2, 'r')
for l in f:
    l = l.strip()
    values = l.split("::", 3) 
    myid = values[0]
    mytext = values[3] # title + description + reviews
    
    pid.append(myid)
    text.append(mytext)
    nrev.append(values[1])
    avgrt.append(values[2])
    
    # get the key_id
    ix = np.where(productids == myid)[0]
    if len(ix)>0:
        kid.append(keyids[ix[0]])
    else:
        kid.append(0)

MySaver('kid4text', kid)        


# Step 1. format, tokenize and lemmatize the text
tic = time.time()
text = [re.sub('\W+',' ',i.lower()) for i in text]
text = [str(unicode(i).encode('utf8')) for i in text] 
wnl = WordNetLemmatizer()
textTokenized = [' '.join([wnl.lemmatize(t) for t in word_tokenize(doc)]) for doc in text]

MySaver('textTokenized', textTokenized)
print "Time elapsed for tokenize/lemmatize the text: %.2f s" % (time.time()-tic)

textTokenized = MyLoader('textTokenized')


# Step 2. tfidf
tic = time.time()
tf = TfidfVectorizer(stop_words='english', min_df=10, max_df=0.3) # only include words appeared at least 10 times in all documents; ignore words appeared in >0.3 of the documents (1 per product)
fitTf = tf.fit(textTokenized)
normText = tf.transform(textTokenized)

print "Shape of the text tfidf matrix: " + str(normText.shape)
print "Number of available elements: %i" % len(normText.data)

words = tf.get_feature_names()

# print words to a file
h = open(datadir + 'textwords.txt', 'w')
for mywords in words:
    h.write("%s\n" % (mywords))
h.close()

print "Number of featured words: %i" % len(words)
print "Time elapsed for tfidf: %.2f s" % (time.time()-tic)

MySaver('words', words)
MySaver('normText', normText)


words = MyLoader('words')
normText = MyLoader('normText')


# Step 3. cosine similarity; a nxn matrix
tic = time.time()
simText = cosine_similarity(normText)
print simText.shape

print "98 percentile: %.3f" % np.percentile(simText[0, :], 98)
print "95 percentile: %.3f" % np.percentile(simText[0, :], 95)
print "50 percentile: %.3f" % np.percentile(simText[0, :], 50)


print "Time elapsed for cosine similarity: %.2f s" % (time.time()-tic)


# Step 4. save to a similarity table 
tic = time.time()

# connect to the database
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# create a new table called simscores with 3 column: item_id1, item_id2, similarityscore
simtable = 'textsimilarityscores'
cur.execute('drop table if exists %s' % simtable)
cur.execute('CREATE TABLE %s (item_id1 INT NOT NULL, item_id2 INT NOT NULL, sim_cosine FLOAT NOT NULL)' % simtable)

# for each product, 
# input: simText (nxn matrix), kid (1d array with n elements)

ordered500 = np.array([np.argsort(i)[-500:] for i in simText]) # top 500 most similar products for each item
rangeNum = range(len(kid))
for i in rangeNum:
    id1 = kid[i]
    
    if id1<1e5: # if movies
        for j in range(500):
            k = ordered500[i,j] # index of the item2
            id2 = kid[k]
            if id2>1e5: # if books
                simscore = simText[i, k]
                cur.execute("INSERT INTO %s (item_id1, item_id2, sim_cosine) VALUES (%i, %i, %.5f)" % (simtable, id1, id2, simscore)) 
        

db.commit()
db.close()


Save2File(simtable) # save to csv file

print "Time elapsed to build the similarity table: %.2f" % (time.time() - tic)












    