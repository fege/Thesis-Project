
def rpcmethod(func):
        """ Decorator to expose Node methods as remote procedure calls
       
        Apply this decorator to methods in the Node class (or a subclass) in order
        to make them remotely callable via the DHT's RPC mechanism.
        """
        func.rpcmethod = True
        return func