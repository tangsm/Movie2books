"""
    Fetch authors, genres, desciptions, images and Facebook pages for books using Amazon, Bing, Goodreads, and Facebook APIs    
    
    Sumin Tang, June 18, 2014
"""

import facebook
import amazonproduct
import goodreads
from pattern.web import Bing, IMAGE
import urllib2, requests, json, re, unicodedata
import os, time, random
import numpy as np
from pattern import web
import MySQLdb
from snap_utils import *



def getrealasin(asin):
    '''
        Problem encountered:
        Many products/ASINs are not available through Amazon API, some of which due to that Amazon changed the ASIN without notifying API. So it returns error msg.
        see e.g. http://stackoverflow.com/questions/22098923/amazon-product-api-itemlook-is-returning-invalid-itemid-even-though-it-is-valid
        One of my case:
        http://www.amazon.com/dp/B000NYROTI/
    '''
    url = "http://www.amazon.com/dp/" + str(asin)
    r = requests.get(url)
    results = web.Element(r.text).by_id('ASIN')
    if results is None:
        newasin = asin
    else:
        newasin = results.attr['value']
    time.sleep(1 + random.random())
    return newasin


def findAuthor(asin):
    # find authors of a book by its product_id
    config = {
    'access_key': '',
    'secret_key': '',
    'associate_tag': '',
    'locale': 'us'
    }
    api = amazonproduct.API(cfg=config)
    item = api.item_lookup(asin)
    itematt = item.Items.Item.ItemAttributes
    if hasattr(itematt, 'Author'):
        author = itematt.Author
    else:
        author = 'unknown'
    time.sleep(1 + random.random()*2)
    return author


def fetchcoverimage(asin):
    # fetch the cover image at amazon by product_id
    # some book cover images are missing, such as Hobbit: http://www.amazon.com/dp/B000QBUCVK
    
    imgdir = './bookimages/amazon/'
    
    config = {
    'access_key': '',
    'secret_key': '',
    'associate_tag': '',
    'locale': 'us'
    }
    params = {'ResponseGroup' : 'Images', 'IdType' : 'ASIN'}
    api = amazonproduct.API(cfg=config)
    try:
        img = api.item_lookup(asin, **params)
        imgattr = img.Items.Item.attrib
        if hasattr(itematt, 'LargeImage'):
            imgurl = img.Items.Item.LargeImage.URL.pyval
            name, ext = os.path.splitext(imgurl)
            fname = '%s%s' % (asin, ext)

            fp = open(imgdir + fname, 'wb')
            fp.write(urllib2.urlopen(imgurl).read())
            fp.close()
            return fname
    except:
        print "Error in ASIN number!"
    time.sleep(1 + random.random()*2)
    
        
def fetchimagefrombing(asin, author, title):
    imgdir = './bookimages/bing/'
    if author == 'unknown':
        author = ''
    name = author + ' ' + title
            
    try:
        result = Bing().search(name+ ' book', type=IMAGE)
    
        try:
            imgurl = result[0].url
            
            try:
                page = urllib2.urlopen(imgurl)
                name, ext = os.path.splitext(imgurl)
                ext2 = ext.split('?')[0]
                fname = '%s%s' % (asin, ext2)

                fp = open(imgdir + fname, 'wb')
                fp.write(page.read())
                fp.close()
    
            except:
                print "Error in retrieving URL!"
        except:
            print "URL not found!"
    except:
        print "Error in Bing search!"
                    
    time.sleep(3 + random.random()*3)


def fetchimage2ways(asin, author, title):
    ''' 
        try amazon cover image first, and if not available, use bing
    '''
    imgdir = './bookimages/amazon/'
    
    config = {
    'access_key': '',
    'secret_key': '',
    'associate_tag': '',
    'locale': ''
    }
    params = {'ResponseGroup' : 'Images', 'IdType' : 'ASIN'}
    api = amazonproduct.API(cfg=config)
    try:
        img = api.item_lookup(asin, **params)
        try:
            imgurl = img.Items.Item.LargeImage.URL.pyval
            name, ext = os.path.splitext(imgurl)
            fname = '%s%s' % (asin, ext)
            
            fp = open(imgdir + fname, 'wb')
            fp.write(urllib2.urlopen(imgurl).read())
            fp.close()
        except:
            print "Amazon cover image not available. Try Bing instead..."
            fetchimagefrombing(asin, author, title)
    except:
        print "Error in ASIN number! Try Bing instead..."
        fetchimagefrombing(asin, author, title)
                
    time.sleep(1 + random.random()*2)



def fetchdes(author, title):
    '''
        fetch book description from goodreads
    '''
    des = 'unknown'
    client = goodreads.Client(client_id="", client_secret="") 
    try:
        book = client.book_title(author=author, title=title)
        des = str(unicode(book.description).encode('utf8'))        
        if len(des)> 10:
            print "fetch book description from goodreads."
        else:
            print "No description available"
    except:
        print "Can't find it at goodreads."
    time.sleep(1 + random.random()*2)
    return des



def fetchgenres(author, title):
    '''
        fetch genres (top-shelves) from goodreads
    '''
    mygenres = ""
    client = goodreads.Client(client_id="", client_secret="") 
    try:
        book = client.book_title(author=author, title=title)
        s = book.popular_shelves
        for mys in s['shelf']:
            myname = mys['@name']
            #mystr = unicode(myname, 'latin-1')
            #myname = unicodedata.normalize('NFKD', mystr).encode('ASCII', 'ignore')
            mygenres = mygenres + str(myname) + "; "
    except:
        print "Can't find it at goodreads."
    
    time.sleep(1 + random.random()*2)
    return mygenres



def getfbid(title, author):
    '''
        Get facebook page id and the total number of likes for a given book.
        
        Example use:
        title = 'Foundation'
        author = 'Asimov'
        myid, nlike = getfbid(title, author)        
        print myid, nlike
    '''
    
    ACCESS_TOKEN = ""
    g = facebook.GraphAPI(ACCESS_TOKEN)
    
    results = g.request('search', {'q' : '%s %s' % (title, author), 'type' : 'page', 'limit' : 5})
    time.sleep(1 + random.random()*2)
    myid = '0'
    nlike = 0
    try:
        myid = results['data'][0]['id']
        nlike = g.get_object(myid)['likes']
        time.sleep(1 + random.random()*2)
    except:
        print "Not available."
    
    return myid, nlike




"""
# ==== step 1. grab the correct ASIN: it takes 2h

tic = time.time()

# load the book info from list file (clean3b)
listfile = "./data/Books_clean3d.txt"
dtype = [('product_id', '|S20'), ('n_review', 'i'), ('avgrating', 'f'), ('title', '|S200')]
pid, n_review, avgrating, titles = np.loadtxt(listfile, dtype = dtype, delimiter='::', unpack = True)

newfile = "./data/Books_clean4a.txt"
h = open(newfile, 'w')
# fetchnew asin ids
for i in range(len(pid)):
    asin = pid[i]
    newasin = getrealasin(asin)
    print asin, newasin
    mytitle = titles[i].split('[', 1)[0].split('(', 1)[0].strip().title()
    mytitle = re.sub("['\"]", '', mytitle).strip()
    h.write("%s::%s::%i::%.3f::%s\n" % (asin, newasin, n_review[i], avgrating[i], mytitle))
h.close()


print "Time Elapsed for grabbing the correct ASIN: %.2f" % (time.time() - tic)


# ==== step 2. grab the author names: it takes 1h

tic = time.time()

# load the book info from list file 
listfile = "./data/book_asins_old_new.txt"
dtype = [('oldproduct_id', '|S20'), ('product_id', '|S20')]
oldpid, pid = np.loadtxt(listfile, dtype = dtype, unpack = True)

newfile = "./data/Books_authors.txt"
h = open(newfile, 'a')

for i in range(1466, len(pid)):
    asin = pid[i]
    author = str(findAuthor(asin))
    author = author.title().strip()
    author = re.sub("[;]", ',', author)
    print author
    h.write("%s::%s::%s\n" % (oldpid[i], asin, author))
h.close()

print "Time Elapsed for grabbing the authors: %.2f" % (time.time() - tic)


# ==== step 3. fetch the cover images from Amazon and Bing

tic = time.time()
listfile = "./data/Books_clean4_authors.txt"
dtype = [('oldproduct_id', '|S20'), ('product_id', '|S20'), ('n_review', 'i'), ('avgrating', 'f'), ('author', '|S200'), ('title', '|S200')]
oldpid, pid, n_review, avgrating, authors, titles = np.loadtxt(listfile, dtype = dtype, delimiter='::', unpack = True)

for i in range(691, len(pid)):
    asin = pid[i]
    myauthor = authors[i]
    mytitle = titles[i]
    print i, asin, myauthor, mytitle
    fetchimage2ways(asin, myauthor, mytitle)

print "Time Elapsed for fetching images from Amazon and Bing: %.2f" % (time.time() - tic)



# ============ remove non-ascii strings ==========

# connect to MySQL
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# create new table
newtable = "Books5"
cur.execute('drop table if exists %s' % newtable)
cur.execute('CREATE TABLE %s (key_id INT NOT NULL, product_id VARCHAR(20), n_review INT NOT NULL, avg_rating FLOAT NOT NULL, imagename VARCHAR(100) NOT NULL, title VARCHAR(200) NOT NULL, author VARCHAR(200) NOT NULL, description VARCHAR(2000) NOT NULL)' % newtable)

cur.execute('select * from Books4')
results = cur.fetchall()
for result in results:
    mykeyid = int(result[0])
    mypid = result[1]
    mynrev = int(result[2])
    myavgrt = float(result[3])
    myimgname = result[4]
    mystr = unicode(result[5], 'latin-1')
    mytitle = unicodedata.normalize('NFKD', mystr).encode('ASCII', 'ignore')
    mystr = unicode(result[6], 'latin-1')
    myauthor = unicodedata.normalize('NFKD', mystr).encode('ASCII', 'ignore')
    mystr = unicode(result[7], 'latin-1')
    mydes = unicodedata.normalize('NFKD', mystr).encode('ASCII', 'ignore')
    
    cur.execute('''INSERT INTO %s (key_id, product_id, n_review, avg_rating, imagename, title, author, description) VALUES (%i, "%s", %i, %.3f, "%s", "%s", "%s", "%s")''' % (newtable, mykeyid, mypid, mynrev, myavgrt, myimgname, mytitle, myauthor, mydes)) 

db.commit()
db.close()


Save2File(newtable) # save to csv file


# =========== Step 4. fetch genres from goodreads
# connect to MySQL
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# create new table
newtable = "Books6"
cur.execute('drop table if exists %s' % newtable)
cur.execute('CREATE TABLE %s (key_id INT NOT NULL, product_id VARCHAR(20), n_review INT NOT NULL, avg_rating FLOAT NOT NULL, imagename VARCHAR(100) NOT NULL, title VARCHAR(200) NOT NULL, author VARCHAR(200) NOT NULL, description VARCHAR(2000) NOT NULL, genres VARCHAR(500) NOT NULL)' % newtable)

cur.execute('select * from Books5')
results = cur.fetchall()
for result in results:
    mykeyid = int(result[0])
    mypid = result[1]
    mynrev = int(result[2])
    myavgrt = float(result[3])
    myimgname = result[4]
    mytitle = str(result[5])
    myauthor = str(result[6])
    mydes = result[7]
    
    # fetch genres from goodreads
    mygenres0 = fetchgenres(myauthor, mytitle)
    mygenres = re.sub('["]', '\'', mygenres0).strip()   
    print myauthor, ', ', mytitle, ', ', mygenres
    
    cur.execute('''INSERT INTO %s (key_id, product_id, n_review, avg_rating, imagename, title, author, description, genres) VALUES (%i, "%s", %i, %.3f, "%s", "%s", "%s", "%s", "%s")''' % (newtable, mykeyid, mypid, mynrev, myavgrt, myimgname, mytitle, myauthor, mydes, mygenres)) 

db.commit()
db.close()


Save2File(newtable) # save to csv file


# =========== Step 5. fetch facebook page id and number of ids for books
# connect to MySQL
config_file = './sql/general.mysql'
db = MySQLdb.connect(read_default_file=config_file)
cur = db.cursor()
cur.execute('USE linkbookmovie')

# create new table
newtable = "Books6fb"
cur.execute('drop table if exists %s' % newtable)
cur.execute('CREATE TABLE %s (facebook_id VARCHAR(30) NOT NULL, facebook_likes INT NOT NULL, key_id INT NOT NULL, product_id VARCHAR(20), n_review INT NOT NULL, avg_rating FLOAT NOT NULL, imagename VARCHAR(100) NOT NULL, title VARCHAR(200) NOT NULL, author VARCHAR(200) NOT NULL, description VARCHAR(2000) NOT NULL, genres VARCHAR(500) NOT NULL)' % newtable)

# also write to file
h = open('./data/book6_facebook.txt', 'w')

cur.execute('select * from Books6')
results = cur.fetchall()
for i in range(len(results)):
    result = results[i]
    mykeyid = int(result[0])
    mypid = result[1]
    mynrev = int(result[2])
    myavgrt = float(result[3])
    myimgname = result[4]
    mytitle = str(result[5])
    myauthor = str(result[6])
    mydes = result[7]
    mygenres = result[8]
    
    # fetch from facebook
    authorlstname = myauthor.split()[-1]
    fbid, nfblike = getfbid(mytitle, authorlstname)
    print i, myauthor, ', ', mytitle, ', ', fbid, nfblike
    
    h.write("%s::%i::%i::%s::%i::%.3f::%s::%s::%s::%s::%s\n" % (fbid, int(nfblike), mykeyid, mypid, mynrev, myavgrt, myimgname, mytitle, myauthor, mydes, mygenres))
    
    cur.execute('''INSERT INTO %s (facebook_id, facebook_likes, key_id, product_id, n_review, avg_rating, imagename, title, author, description, genres) VALUES ("%s", %i, %i, "%s", %i, %.3f, "%s", "%s", "%s", "%s", "%s")''' % (newtable, fbid, int(nfblike), mykeyid, mypid, mynrev, myavgrt, myimgname, mytitle, myauthor, mydes, mygenres)) 

db.commit()
db.close()
h.close()

Save2File(newtable) # save to csv file
"""
