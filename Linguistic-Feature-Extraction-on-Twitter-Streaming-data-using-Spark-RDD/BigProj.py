import  json


from requests import request
import requests

from tweepy import OAuthHandler
from tweepy import StreamingClient,StreamRule



# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="8IbGqpDXzXk3S1J2zc3aQL4Ux"
consumer_secret="ww6Wt4R7zIyGXNWI4qf0Ctp5bZsFVdNUAAdkfapYdfPwAWO2kL"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="1510717193213861898-bovra4j1Vgn5hUt8ETjqDt35U9mugI"
access_token_secret="8xjFql5HjgfmIjRqpCxDrSCTGj5orFLiD7bvTwc9onO5Q"

bearer_token="AAAAAAAAAAAAAAAAAAAAAL5fbAEAAAAAXizdiQ6J0bT6L0NOHNLC%2Ft8zAu0%3DgrM8tjExOEZLaGiTKPGf2bunCWuUHHugzgzGG2tzC1vHN9cuK8"


class StdOutListener(StreamingClient):
    """ A listener handles tweets that are received from the stream.
    This is tweets_counta basic listener that just prints received tweets to stdout.
    """


    def on_data(self, data):
        message = json.loads(data)
        print(message)
        return True

    

    def on_exception(self, exception):
        print(exception)
        return

    def on_error(self, status):
        print(status)

    def on_status(self, status):
        return
    
    def on_connection_error(self):
        self.disconnect()

if __name__ == '__main__':
    # twitter_stream = StdOutListener(consumer_key,consumer_secret,
    #                                 access_token,access_token_secret)   



    twitter_stream_client = StdOutListener(bearer_token,return_type = requests.Response ,wait_on_rate_limit=True)
    
    # auth = OAuthHandler(consumer_key, consumer_secret)
    # auth.set_access_token(access_token, access_token_secret)
    # stream = Stream(auth, l)
   # msg = raw_input('Enter search string? ')
    #query=("[\'"+msg+"\']")
    #print(query)
    # twitter_stream.filter(track=['trump'], stall_warnings=True)

    # twitter_stream_client.add_rules(StreamRule("Trump"))
    # twitter_stream_client.filter()

    if(twitter_stream_client.get_rules().json()['data'][0]['value'] == 'Trump'):
       print("The keyword token exists. Hence starting the stream.")
       twitter_stream_client.filter()
    else:
        print("The keyword token does not exists. Hence adding it and starting the stream.")
        twitter_stream_client.add_rules(StreamRule("Trump"))
        twitter_stream_client.filter()