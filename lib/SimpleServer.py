
import socket
import redis
import logging
import threading

logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-15s %(levelname)-8s %(filename)-16s %(lineno)4d %(message)s')

class Client:
    def __init__(self, socket1=None, address1=None):
        self.s = socket1
        self.address = address1

    def publish(self, instruction):
        try:
            self.s.send(instruction)
        except socket.error:
            print("became disconnected")

class Listener(threading.Thread):
    def __init__(self, hostname, port1, channel):
        threading.Thread.__init__(self)
        self.redis = redis.StrictRedis(host=hostname, port=port1, db=0)
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channel)
        self.clients = list()
    
    def addClient(self, client):
        logging.info("added client to redis listener")
        self.clients.append(client)
    
    def work(self, message):
        instruction = message['data']
        logging.info("caught instruction {0} sending to {1} clients".format(instruction, len(self.clients)))
        for client in self.clients:
            client.publish(instruction)
    
    def run(self):
        for item in self.pubsub.listen():
            self.work(item)


class SimpleServer(object):
    def __init__(self, port1=50041):
        self.s = socket.socket()
        self.host = socket.gethostname()
        self.port = port1
        self.clients = list()
        self.watchKey = None
        self.redisConnection = None
        self.useRedis = False
        self.listener = None

    def initRedis(self, hostname, watch, port1=6379):
        self.useRedis = True
        self.redisConnection = redis.StrictRedis(host=hostname, port=port1, db=0)
        self.watchKey = watch
        self.listener = Listener(hostname, port1, watch)
        self.listener.start()

    def serve(self):
        self.s.bind((self.host, self.port))
        try:
            while True:
                self.s.listen(5)
                c, addr = self.s.accept()
                identifier = c.recv(10)
                if(identifier.find('P') > -1):
                    instruction = None
                    if(len(identifier) > 1):
                        instruction = identifier.replace('P', '')
                    else:
                        instruction = c.recv(10)

                    if(not self.useRedis):
                        for client in self.clients:
                            client.publish(instruction)
                    elif(self.watchKey):
                        logging.info("publishing {0} to key {1} via redis {2}".format(instruction,self.watchKey,self.redisConnection))
                        self.redisConnection.publish(self.watchKey, instruction)

                if(identifier.find('C') > -1):
                    if self.listener is None:
                        self.clients.append(Client(c, addr))
                    if self.listener is not None:
                        self.listener.addClient(Client(c, addr))
                        
        except KeyboardInterrupt:
            for client in self.clients:
                client.s.close()
            print("exiting")
            self.s.close()

if __name__ == "__main__":
    logging.info("starting server")
    s = SimpleServer(50041)
    s.serve()
