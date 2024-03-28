from db import Page,TfIdf
from mongoengine import connect 
from threading import Thread 
connect("search")
TfIdf.calculate_tfidf_for_pages()