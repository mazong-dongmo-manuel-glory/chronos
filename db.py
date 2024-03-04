from mongoengine.fields import URLField,StringField,ListField,DateField,ObjectIdField,DictField
from mongoengine.document import Document
from datetime import datetime


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
            
            