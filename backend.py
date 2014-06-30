'''
    Fetch top recommended books for Movie2Books.
    
    Sumin Tang, June 2014
'''

import MySQLdb

# recommend books by from a given movie
def recommender(title):
    config_file = './general.mysql'
    db = MySQLdb.connect(read_default_file=config_file)
    cur = db.cursor()
    query = "select distinct title, imagename, author, description, s.score, product_id, facebook_id from Books6fb as b \
               join \
                 (select item_id2, max(simgen_score) as score \
                  from textsimilarityscores \
                  where \
                  (item_id1 in (select key_id from Movies_clean3_id where title like '%%%s%%')) \
                  and (item_id2>100000) \
                  group by item_id2 \
                  order by score desc limit 20) as s \
               on b.key_id = s.item_id2 \
               order by s.score desc limit 5" % title
    cur.execute(query)
    results = cur.fetchall()
    books = []
    for result in results:
        book = []
        book.append(result[0]) # title
        book.append(result[1]) # imageurl
        book.append(result[2]) # author
        book.append(result[3]) # description
        book.append(result[5]) # amazon id
        book.append(result[6]) # facebook id
        books.append(book)
    return books

