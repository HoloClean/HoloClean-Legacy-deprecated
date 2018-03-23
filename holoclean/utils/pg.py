import psycopg2

try:
    conn = psycopg2.connect("dbname='holo' user='holocleanuser' host='localhost' password='abcd1234'")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
cur.execute("""CREATE TABLE tryfromholo (a int, b int)""")

cur.execute("""INSERT INTO tryfromholo values (1,1)""")

cur.execute("""select * from tryfromholo""")

conn.close()

conn.
cur.

rows = cur.fetchall()

for row in rows:
    print "   ", row[0]

    row = cur.
