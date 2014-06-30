#!/Users/litfeix/anaconda/bin/python
"""
    Some utility functions

    
    Sumin Tang, June 12, 2014
"""

import nltk
import MySQLdb
import re
import csv
import cPickle as pickle


def isDuplicateTitle(titleA, titles):
    '''
        Check whether a title is in a list of titles
    '''
    for titleB in titles:
        if TitleOverlap(titleA, titleB):
            return True
    return False


def TitleOverlap(titleA, titleB):
    '''
        Use nltk to see whether two titles are the same product.
    '''
    titleA = titleA.split('[', 1)[0].split('(', 1)[0] # ignore anything after "[" or "("
    titleA = re.sub('[&-]', ' ', titleA).strip().lower()
    tokensA = nltk.word_tokenize(titleA)
    
    titleB = titleB.split('[', 1)[0].split('(', 1)[0]
    titleB = re.sub('[&-]', ' ', titleB).strip().lower()
    tokensB = nltk.word_tokenize(titleB)
    
    ComTitle = list(set(tokensA).intersection(set(tokensB)))
    
    minlen = min(len(set(tokensA)), len(set(tokensB)))
    
    if minlen >= 5:
        if len(ComTitle) >= (minlen-2):
            return True
        else:
            return False
    elif minlen >=3:
        if len(ComTitle) >= (minlen-1):
            return True
        else:
            return False
    else:
        if len(ComTitle) >= minlen:
            return True
        else:
            return False
    
    

def findyear(title):
    # find the year in a movie title in ()
    years = filter(str.isdigit, title)
    year = '0'
    try:
        if (int(years)>1900) and (int(years)<2014): 
            year = years
    except:
        pass
    return year


def Save2File(tablename):
    '''
        save a MySQL table to csv file
    '''
    config_file = './sql/general.mysql'
    db = MySQLdb.connect(read_default_file=config_file)
    cur = db.cursor()
    cur.execute('USE linkbookmovie')

    cur.execute("SELECT * FROM %s" % tablename)
    rows = cur.fetchall()
    fp = open('/Users/litfeix/Insight/myproject/snapamazon/data/%s.csv' % tablename, 'w')
    myFile = csv.writer(fp)
    myFile.writerows(rows)
    fp.close()
    db.close()


def MyLoader(name):
    datadir = "./data/"
    ifile = open(datadir + name+'.pkl','rb')
    temp = pickle.load(ifile)
    ifile.close()
    return temp

def MySaver(name, thing):
    datadir = "./data/"
    ofile = open(datadir + name + '.pkl','wb')
    pickle.dump(thing,ofile)
    ofile.close()
    return


def smarttruncatewords(content, length=100, suffix='...'):
    if len(content) <= length:
        return content
    else:
        return ' '.join(content[:length+1].split(' ')[0:-1]) + suffix


def smarttruncatesen(content, length=1000, suffix='...'):
    if len(content) <= length:
        return content
    else:
        return '. '.join(content[:length+1].split('. ')[0:-1]) + suffix

