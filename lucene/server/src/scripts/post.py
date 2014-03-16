import urllib2

r = urllib2.urlopen('http://localhost:4000/updateDocument', 'here is some data')
print r.read()


