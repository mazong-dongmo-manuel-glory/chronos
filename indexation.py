from db import  Score 
from mongoengine.connection import connect
connect("search")
Score.calculate_tfidf()