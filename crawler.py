import requests 
from urllib.parse import urlparse 
from bs4 import BeautifulSoup 
import threading
import re 
from mongoengine.connection import connect
from db import Page,UrlFile
import time
import validators
connect("search")
class Data:
    QUEUE = []
    LOCK = threading.Lock()
    IN_PROCESS = dict()
    QUEUE_SIZE = 1000
    QUEUE_SIZE_INTER = 100
    NUMBER_OF_THREAD = 0
    MAX_THREAD = 10
class Crawler:

    def __init__(self,key=0) -> None:
        Data.LOCK.acquire()
        Data.NUMBER_OF_THREAD += 1 
        Data.LOCK.release()
        self.key = key
        pass
    
    def addToQueue(self,url):
        Data.LOCK.acquire()
        if type(url) == list:
            Data.QUEUE =  Data.QUEUE + url
        else:
            Data.QUEUE.append(url)
        Data.LOCK.release()
        
    def getToQueue(self):
        Data.LOCK.acquire()
        if len(Data.QUEUE)<=0:
            url = None
        else:
            url = Data.QUEUE.pop(0)
        
        
        Data.LOCK.release()
        return url
    def getInProcess(self,url):
        Data.LOCK.acquire()
        etat = url in Data.IN_PROCESS 
        Data.LOCK.release()
        return etat
    def addInProcess(self,url):
        Data.LOCK.acquire()
        Data.IN_PROCESS[url] = time.time()
        Data.LOCK.release()
    def removeInProcess(self,url):
        Data.LOCK.acquire()
        try:
            del Data.IN_PROCESS[url]
        except:
            pass
        Data.LOCK.release()
    
    def addNumberOfThread(self):
        Data.LOCK.acquire()
        Data.NUMBER_OF_THREAD += 1 
        Data.LOCK.release()
    
    def minusNumberOfThread(self):
        Data.LOCK.acquire()
        Data.NUMBER_OF_THREAD -= 1 
        Data.LOCK.release()
    
    def crawl(self,root_url):
        self.addToQueue(root_url)
        while len(Data.QUEUE)>0:
            url = self.getToQueue()
            if not self.getInProcess(url) and Data.NUMBER_OF_THREAD < Data.MAX_THREAD:
                self.addInProcess(url)
                cr = Crawler(key=Data.NUMBER_OF_THREAD)
                th = threading.Thread(target=cr.crawl,args=(url,))
                th.start()
                self.addNumberOfThread()
                continue
            #on verifie si la page existe deja
            if Page.get(url):
                self.removeInProcess(url)
                continue
            page = self.getPage(url)
            if  not page:
                self.removeInProcess(url)
                continue
            self.addToQueue(page["links"])
            if Page.insert(page["url"],page["content"],page["links"]):
                print(f"{self.key} {url}")
            self.removeInProcess(url)
        self.removeInProcess(url)
        self.minusNumberOfThread()
        
            
            
    @classmethod
    def handleQueue(cls):
        Data.QUEUE_SIZE = int(Data.QUEUE_SIZE / Data.MAX_THREAD)
        Data.QUEUE_SIZE_INTER = int(Data.QUEUE_SIZE/Data.MAX_THREAD)
        def handle():
            while True:
                if len(Data.QUEUE) > Data.QUEUE_SIZE:
                    Data.LOCK.acquire()
                    to_save = Data.QUEUE[:len(Data.QUEUE)-Data.QUEUE_SIZE_INTER]
                    Data.QUEUE = Data.QUEUE[len(Data.QUEUE)-Data.QUEUE_SIZE_INTER:]
                    Data.LOCK.release()
                
                    for link in to_save:
                        UrlFile.insert(link)
                    
                if len(Data.QUEUE) < Data.QUEUE_SIZE/Data.MAX_THREAD:
                    cls.insertUrlInQueue()
              
                
        threading.Thread(target=handle).start()
    @classmethod
    def insertUrlInQueue(cls):
            Data.LOCK.acquire()
            Data.QUEUE += UrlFile.get(Data.QUEUE_SIZE*Data.MAX_THREAD)
            Data.LOCK.release()
        
        
    def validate_domain(self,domain):
        pattern = re.compile(
        r'^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$')
        return bool(pattern.match(domain))
    
    def validate_path(self,path):
        pattern = re.compile(
        r'^(/[^/ ]*)+/?$'
        )
        return bool(pattern.match(path))
       #retourne tous les liens et le contenu de la page
    def getPage(self,url:str)->bool|dict:
        response = None
        page  =  None
        try:
            response = requests.get(url)
            if response.status_code != 200 or not response.headers["Content-Type"].startswith("text/html"):
                return False 
        except Exception as e:
            return False
        try:
            page = BeautifulSoup(response.content,"html.parser")
        except Exception as e:
            return False 
        links = set()
        for link in page.find_all("a"):
            link = urlparse(link.get("href"))
            if not link.geturl():
                continue
            if link.netloc and link.netloc != "" and link.scheme and link.scheme != "":
                links.add(link.geturl())
                continue
            elif not link.scheme and self.validate_domain(link.geturl()) and validators.url("https://"+link.geturl()):
                links.add("https://"+link.geturl())
                continue 
            elif self.validate_path(link.geturl()):
                if url.endswith("/") and link.geturl().startswith("/"):
                    if validators.url(url[:len(url)-1]+link.geturl()):
                        links.add(url[:len(url)-1]+link.geturl())
                    continue
                else:
                    if validators.url(links.add(url+link.geturl())):
                        links.add(links.add(url+link.geturl()))
                    
                continue
            elif link.query.startswith("?") and validators.url(url+link.query):
                links.add(url+link.query)
                continue
        content = ""
        last_word = ""
        for word in page.get_text().split():
            if last_word != " ":
                content = content + " " + word 
            else:
                content += word 
            last_word = word 
            
        return {"url":url,"links":list(links),"content":content}
    
cr = Crawler()

Crawler.handleQueue()

cr.crawl("https://fr.wikipedia.com")