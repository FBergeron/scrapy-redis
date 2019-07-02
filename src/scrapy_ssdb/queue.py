from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.request import request_fingerprint
from . import picklecompat

# TODO: The prefix should not be hardcoded.
TABLE_REQUEST_BODIES = 'blog_crawler:request_bodies'

class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, spider, key, serializer=None):
        """Initialize per-spider ssdb queue.

        Parameters
        ----------
        server : ssdb3.Client
            SSDB client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            SSDB key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.serializer = serializer

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.qclear(self.key)


class FifoQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        return self.server.qsize(self.key)

    def push(self, request):
        """Push a request"""
        self.server.qpush_front(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """Pop a request"""
        data = self.server.qpop_back(self.key)
        if data:
            return self._decode_request(data[0])


class PriorityQueue(Base):
    """Per-spider priority queue abstraction using ssdb' sorted set"""

    def __len__(self):
        """Return the length of the queue"""
        return self.server.zsize(self.key)

    def push(self, request):
        """Push a request"""
        data = self._encode_request(request)
        score = -request.priority
        fingerprint = request_fingerprint(request)        

        self.server.hset(TABLE_REQUEST_BODIES, fingerprint, data)
        self.server.zset(self.key, fingerprint, score)

    def pop(self, timeout=0):
        """
        Pop a request
        timeout not support in this queue class
        """
        items = self.server.zpop_front(self.key, 1)
        if items is not None and isinstance(items, list) and len(items) > 0:
            fingerprint = items[0].decode('utf-8')
            score = int(items[1].decode('utf-8'))
            if fingerprint is not None:
                data = self.server.hget(TABLE_REQUEST_BODIES, fingerprint)
                self.server.hdel(TABLE_REQUEST_BODIES, fingerprint)
                request = self._decode_request(data)
                return request


class LifoQueue(Base):
    """Per-spider LIFO queue."""

    def __len__(self):
        """Return the length of the stack"""
        return self.server.qsize(self.key)

    def push(self, request):
        """Push a request"""
        self.server.qpush_front(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """Pop a request"""
        data = self.server.qpop_front(self.key)
        if data:
            return self._decode_request(data[0])


# TODO: Deprecate the use of these names.
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue
