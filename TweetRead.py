# https://developer.twitter.com/en/portal/dashboard den Create Project tiklaniyor ve application twitter hesabiniza baglanarak ayarlari yapiliyor
# pip install tweepy==3.10.0

# python TweetRead.py

import tweepy
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import time
import os
import socket
import json

api_key = 'qAAwbfFy0D8eq63EiFqtIzn7v'
api_key_secret ='HXoMLa7DZzn6zp5KoXokLOz8OItiMaagpgfS9HeV5sKmom6ATN'
access_token = '1426949257815265288-91Zvzdhs89u62BYj8OglwkXgtPJFrS'
access_secret = 'E5XwVdfJp8VvqwcZYafdYr3UaNXv8IDlMnbB4G8WwEIfG'

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(api_key, api_key_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['erdogan'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "localhost"      # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )
