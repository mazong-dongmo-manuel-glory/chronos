from requests import get
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from threading import Thread
from time import sleep
from urllib.parse import urlparse
from utils import is_valid_url
from data import Data
from db import Page


class Crawler:

    def __init__(self, url: str):
        self.root = url

    def crawl(self):
        Data.addThread()
        id = Data.NUMBER_OF_THREADS
        while True:
            url = Data.getInQueue()
            if not url:
                sleep(1)
                continue
            if Data.getInProcess(url):
                continue
            page = self.getPage(url)
            Data.addInProcess(url)
            if not page or Page.get(url):
                Data.removeInProcess(url)
                continue
            Data.addQueue(page["urls"])
            if Page.insert(title=page["title"],url=page["url"],content=page["content"],urls=page["urls"]):
                print(f"{url} {id} ")
            Data.removeInProcess(url)
            
            
                

    def getPage(self, url: str) -> dict | None:
        if not is_valid_url(url):
            return
        urlp = urlparse(url, allow_fragments=True)
        scheme = urlp[0]
        netloc = urlp[1]
        path = urlp[2]
        query = urlp[3]
        try:
            robots = RobotFileParser()
            robots.set_url(scheme + "://" + netloc + "/robots.txt")
            robots.read()
            if not robots.can_fetch("*", url):
                return
        except:
            return
        response = None
        try:
            response = get(url, allow_redirects=True,headers={"User-Agent":"MazongBot"})
            if response.status_code != 200 or response.headers["Content-Type"].split(";")[0] != "text/html":
                return
        except:
            return
        soup = None
        try:
            soup = BeautifulSoup(response.text, "html.parser")
        except:
            return
        links = set()
        for link in soup.find_all("a"):
            link = link.get("href")
            parsed_url = urlparse(link, allow_fragments=True)
            newLink = None
            if parsed_url[0] != "" and parsed_url[1] != "":
                newLink = parsed_url.geturl()
            elif parsed_url[1] != "":
                newLink = "https://" + parsed_url.geturl()
            elif parsed_url[2].startswith("/"):
                newLink = scheme + "://" + netloc + parsed_url.geturl()
            if newLink:
                if is_valid_url(newLink):
                    links.add(newLink)
        title = soup.find("title")
        if title:
            title = title.get_text()
        return {
            "url": url,
            "content": self.formatContent(soup),
            "title": title,
            "urls": list(links)
        }

    def formatContent(self, soup: BeautifulSoup):
        content = soup.get_text(strip=True).encode("utf-8")
        return content

    def start(self):
        Data.addQueue([self.root])
        for _ in range(Data.MAX_THREAD):
            th = Thread(target=self.crawl)
            th.start()
        
        
    
