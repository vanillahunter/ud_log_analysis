import psycopg2

# connect to news database
db = psycopg2.connect(dbname="news")
c = db.cursor()

# JOIN log with articles to find all requests that lead to all articles
# THEN GROUP BY articles and ORDER BY counts and LIMIT output to 3
query1 = "SELECT title, count(*) AS num FROM log JOIN articles "\
"ON path LIKE CONCAT('/article/', slug) GROUP BY title ORDER "\
"BY num DESC LIMIT 3;"
# execute the first query
c.execute(query1)
rows1 = c.fetchall()
# print out all results
print
print "Most Popular Three Articles of All Time:"
for row in rows1:
    print '"{}" -- {} views'.format(row[0], row[1])

# FIRST JOIN log and articles on slug matches path
# select author id and counts from the previuos results
# THEN join again with TABLE authors to get authots' names
query2 = "SELECT name, num FROM authors JOIN (SELECT author, "\
"count(*) AS num FROM log JOIN articles ON path LIKE CONCAT("\
"'/article/', slug) GROUP BY author) AS author_views ON autho"\
"rs.id = author_views.author ORDER BY num DESC;"
# execute the second query
c.execute(query2)
rows2 = c.fetchall()
# print out the results
print
print "Most Popular Article Authors of All Time:"
for row in rows2:
    print '{} -- {} views'.format(row[0], row[1])

# SUM all the error requests in a single day
# count all the requests on that day
# divide them to percentage and select all percentage more than 1%
query3 = "SELECT time, percentage FROM (SELECT CAST(time AS DATE), "\
"CAST(CAST(SUM(CASE WHEN status!= '200 OK' THEN 1 END) AS FLOAT)*"\
"100/CAST(count(*) AS FLOAT) AS DECIMAL(18,2)) AS percentage FROM "\
"log GROUP BY CAST(time AS DATE)) AS date_o WHERE percentage > 1;"
# execute the third query
c.execute(query3)
rows3 = c.fetchall()
# print out reults
print
print "Days Did More Than 1% of Requests Lead to Errors"
for row in rows3:
    print '{} -- {}% errors'.format(row[0], row[1])

db.close
