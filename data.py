from threading import Lock
from time import time
from db import UrlFile
class Data:
    MAX_THREAD = 100
    QUEUE_SIZE = 10000
    QUEUE_MIN = 5000
    IN_PROCESS = {}
    DOMAINS = {}
    QUEUE = set()
    IN_PROCESS_LOCK = Lock()
    QUEUE_LOCK = Lock()
    DOMAIN_LOCK = Lock()
    THREAD_LOCK = Lock()
    NUMBER_OF_THREADS = 0
    OLD_DOMAINS = ""
    DOMAIN = {}
    @classmethod
    def addThread(cls):
        cls.THREAD_LOCK.acquire()
        cls.NUMBER_OF_THREADS += 1
        cls.THREAD_LOCK.release()
    @classmethod
    def reduceThread(cls):
        cls.THREAD_LOCK.acquire()
        cls.NUMBER_OF_THREADS -= 1
        cls.THREAD_LOCK.release()

    @classmethod
    def addQueue(cls, urls: list):
        cls.QUEUE_LOCK.acquire()
        for url in urls:
            if not cls.getInProcess(url):
                cls.QUEUE.add(url)
        cls.QUEUE_LOCK.release()

    @classmethod
    def getInQueue(cls):
        url = None
        cls.QUEUE_LOCK.acquire()
        if len(cls.QUEUE) > 0:
            url = cls.QUEUE.pop()
        cls.QUEUE_LOCK.release()
        return url

    @classmethod
    def addInProcess(cls, url: str):
        cls.IN_PROCESS_LOCK.acquire()
        cls.IN_PROCESS[url] = time()
        cls.IN_PROCESS_LOCK.release()
    @classmethod
    def removeInProcess(cls, url: str):
        cls.IN_PROCESS_LOCK.acquire()
        if url in cls.IN_PROCESS:
            del cls.IN_PROCESS[url]
        cls.IN_PROCESS_LOCK.release()

    @classmethod
    def getInProcess(cls, url: str) -> bool:
        state = False
        cls.IN_PROCESS_LOCK.acquire()
        if url in cls.IN_PROCESS:
            state = True
        cls.IN_PROCESS_LOCK.release()
        return state

    @classmethod
    def handleQueue(cls):
        if len(cls.QUEUE)>cls.QUEUE_SIZE:
            cls.QUEUE_LOCK.acquire()
            to_save = list(cls.QUEUE)[:cls.QUEUE_SIZE]
            cls.QUEUE = set(list(cls.QUEUE)[cls.QUEUE_SIZE:])
            cls.QUEUE_LOCK.release()
            for url in to_save:
                UrlFile.insert(url)
        if len(cls.QUEUE) < cls.QUEUE_MIN:
            urls = UrlFile.get(cls.QUEUE_SIZE)
            cls.QUEUE_LOCK.acquire()
            if not urls:
                for url in urls:
                    cls.QUEUE.add(url)
            cls.QUEUE_LOCK.release()