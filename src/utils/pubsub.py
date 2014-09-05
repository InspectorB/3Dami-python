class PubSub:

    def __init__(self):
        self.subscribers = []
        
    def publish(self, obj):
        for sub in self.subscribers:
            sub.notify(self, obj)
            
    def notify(self, publisher, obj):
        raise NotImplementedError("The method 'notify' should be implemented.")
    
    def subscribe(self, *subscribers):
        self.subscribers.extend(subscribers)
        self.subscribers = list(set(self.subscribers)) # remove doubles
        
    def unsubscribe(self, *subscribers):
        for subscriber in subscribers:
            self.subscribers.remove(subscriber)
        
    def clear_subscribers(self):
        self.subscribers = []
        
    def on_finish(self):
        raise NotImplementedError("The method 'on_finish' should be implemented.")
        
    def finish(self):
        self.on_finish()
        for sub in self.subscribers:
            sub.finish()



class Filter(PubSub):
    """Filters data from the data location"""

    def __init__(self, selector=None, transform=None):
        """
        Arguments:
        selector   -- a function that returns true of a data object if it should be included
        transform  -- a function that maps the data object to something else
        """
        PubSub.__init__(self)
        self.selector = selector
        self.transform = transform or Filter.identity

    def notify(self, publisher, obj):
        if self.selector == None or self.selector(obj):
            self.publish(self.transform(obj))

    @staticmethod
    def identity(x):
        return x
