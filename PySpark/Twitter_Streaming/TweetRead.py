from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import socket
import json


# Set up your credentials
consumer_key='SMZQ5wq4hrH0FfXJSEBLMUJ6U'
consumer_secret='Ng65fNannBmuIVwnc85XSBUPjbvEHrSb05Ljjmx2yB7vTMM60j'
access_token ='23032405-fFRU4fm3JJjDSNJ6JxkxbXVSs4LF09BNWP3OJZxb1'
access_secret='8Tjyv6kLaG9vFm3reuGSmlVGY4PhuriEodWIvSQgGwzmE'


class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads(data)
          print(msg['text'].encode('utf-8'))
          self.client_socket.send(msg['text'].encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['trump'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print("Received request from: %s" % str(addr))

  sendData(c)