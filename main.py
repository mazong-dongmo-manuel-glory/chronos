from mongoengine.connection import connect
from crawler import Crawler
connect("search")

cr = Crawler("https://fr.wikipedia.org/wiki/ChatGPT")
cr.start()