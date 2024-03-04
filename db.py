from mongoengine.fields import URLField,StringField,ListField,DateField,ObjectIdField,DictField
from mongoengine.document import Document
from datetime import datetime

from collections import defaultdict
from math import log
class TfidfScore(Document):
    page_id = ObjectIdField(required=True, unique=True)
    scores = DictField(required=True)

class Indexation(Document):
    page_id = ObjectIdField(required=True, unique=True)
    tfidf_scores = DictField(required=True)
    
    
class UrlFile(Document):
    url = URLField(unique=True)
    timestamp = DateField(default=datetime.now())
    
    @classmethod 
    def insert(cls,url):
        url = UrlFile(url=url)

        try:
            url.validate()
            url.save()
            return True
        except Exception as e:
            return False
    @classmethod 
    def get(cls,limit=20):
        urls = None
        try:
            urls = cls.objects.order_by('timestamp').limit(limit)
            if len(urls) == 0:
                return []
        except:
            return []
        result = [url.url for url in urls]
        for url in urls:
            try:
                url.delete()
            except Exception as e:
                print(f"Failed to delete url ")
        
        return result

class Page(Document):
    url = URLField(unique=True)
    timestamp = DateField(default=datetime.now())
    links = ListField(URLField())
    content = StringField()
    
    @classmethod 
    def insert(cls,url,content,links):
        page = Page(url=url,content=content,links=links)
        
        try:
            page.validate()
        except Exception as e:
            return False
        try:
            page.save()
            return True
        except Exception as e:
            return False
    
    @classmethod 
    def get(cls,url):
        try:
            page = Page.objects(url=url).first()
            if page:
                return True
            return False
        except:
            return False

class Score(Document):
    url = URLField(unique=True)
    tfidf_scores = DictField()

    @classmethod
    def insert_or_update(cls, url, tfidf_scores):
        try:
            score = cls.objects.get(url=url)
            score.tfidf_scores.update(tfidf_scores)
            score.save()
        except cls.DoesNotExist:
            score = Score(url=url, tfidf_scores=tfidf_scores)
            score.save()

    @classmethod
    def calculate_tfidf(cls):
        word_freq_per_page = defaultdict(lambda: defaultdict(int))
        total_pages = Page.objects.count()

        for page in Page.objects:
            words = page.content.split()
            for word in set(words):
                word_freq_per_page[word][page.url] += 1

        idf = {}
        for word, page_freq in word_freq_per_page.items():
            idf[word] = log(total_pages / len(page_freq))

        tfidf_per_page = defaultdict(dict)
        for word, page_freq in word_freq_per_page.items():
            for page, freq in page_freq.items():
                tfidf_per_page[page][word] = freq * idf[word]

        for page_url, tfidf_scores in tfidf_per_page.items():
            Score.insert_or_update(page_url, tfidf_scores)