from mongoengine.fields import StringField, URLField, DictField,ListField
from mongoengine.document import Document
from mongoengine.queryset.visitor import Q
from math import log
from collections import defaultdict
class Page(Document):
    meta = {'collection': 'pages'}
    title = StringField()
    url = URLField(required=True, unique=True)
    urls = ListField(URLField())
    content = StringField()

    @classmethod
    def insert(cls, title: str, url: str, content: str, urls: list) -> bool:
        page = Page(title=title, url=url, urls=urls, content=content)
        try:
            page.validate()
        except:
            return False

        try:
            page.save()
            return True
        except:
            return False

    @classmethod
    def get(cls, url) -> None | Document:
        try:
            page = Page.objects(url=url).first()
            if not page:
                return None
        except:
            return None
    
    
    def calculate_pagerank(self, damping_factor=0.85, max_iterations=100):
        total_pages = Page.objects.count()

        # Initialiser le PageRank pour cette page à 1
        pagerank = {self.url: 1}

        # Itérer et calculer de nouvelles valeurs de PageRank
        for _ in range(max_iterations):
            new_pagerank = (1 - damping_factor) / total_pages
            for linked_url in self.urls:
                linked_page = Page.objects(url=linked_url).first()
                if linked_page and linked_page.urls:  # Vérifier que la page liée existe et a des URLs
                    new_pagerank += damping_factor * pagerank[linked_url] / len(linked_page.urls)
            pagerank[self.url] = new_pagerank

        # Mettre à jour le PageRank pour cette page
        self.pagerank = pagerank[self.url]
        self.save()

class UrlFile(Document):
    url = URLField(unique=True)
    @classmethod
    def insert(cls, url):
        url = UrlFile(url=url)
        try:
            url.validate()
            url.save()
            return True
        except:
            return False
    @classmethod
    def get(cls, limit=100):
        try:
            data = UrlFile.objects().limit(limit)
            r = [result.url for result in data]
            for d in data:
                d.delete()
            return r
        except:
            return False

        

class TfIdf(Document):
    word = StringField(required=True,unique=True)
    urls = DictField()
    
    @classmethod
    def calculate_tfidf_for_pages(cls):
        total_documents = Page.objects.count()
        word_document_count = defaultdict(int)

        # Compter le nombre de documents contenant chaque mot
        for page in Page.objects:
            unique_words = set(page.content.split())
            for word in unique_words:
                word_document_count[word] += 1

        # Parcourir chaque mot dans chaque page
        for word_obj in cls.objects:
            word = word_obj.word
            idf = log(total_documents / word_document_count.get(word, 1))  # Utiliser 1 si le mot n'est pas trouvé

            # Parcourir chaque URL dans le dictionnaire urls
            for url, tf in word_obj.urls.items():
                # Récupérer la page correspondante à l'URL
                page = Page.objects(url=url).first()
                if page:
                    tfidf = tf * idf
                    # Ajouter ou mettre à jour le TF-IDF pour ce mot et cette URL
                    if word not in page.tfidf:
                        page.tfidf[word] = tfidf
                    else:
                        page.tfidf[word] += tfidf
                    # Sauvegarder la page mise à jour
                    page.save()
    
