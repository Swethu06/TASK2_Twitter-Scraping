import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from datetime import datetime as dt
import pytz
import json

#with st.form(key='Twitter_form'):
st.title("Twitter Scrapper")
query=st.text_input("Enter the Keyword or Hashtag to be searched :", value="")
query = query.strip()
limit = st.slider("Choose the limit to be searched :", 1, 10000, step=10)
start_date=st.date_input('Start date',dt.today().date())
end_date = st.date_input('End date', dt.today().date())
#connection to mongo DB
client = MongoClient("mongodb+srv://Swetha:swetha06@cluster0.dbuhby6.mongodb.net/test")
db = client["twitter_db"]
collection = db["twitter_data"]

if start_date  <= end_date:
    if end_date <= dt.today().date():
        pass
    else:
        st.error("End date should be on or before today's date")
else:
    st.error("End date should be on or after start date")

def json_serial(obj):
# check if the passed object(not serializable by default json code) obj is an instance of the datetime class.
    if isinstance(obj, (dt)):
        return obj.isoformat()
    if isinstance(obj, sntwitter.TextLink):
        return obj.url
    if isinstance(obj, sntwitter.UserLabel):
        return obj.user_label
    raise TypeError("Type %s not serializable" % type(obj))


def get_tweets():
    tweets = []
    # access to the Olson time zone database
    timezone = pytz.timezone("UTC")  # Coordinated Universal Time (UTC) time zone.
    # Get the items based on the input
    if query.strip() == "":
        st.error("Please enter a valid keyword or hashtag")
        return
    for tweet in sntwitter.TwitterSearchScraper(query).get_items():
        if len(tweets) == limit:
            break
        else:
            # __dict__access the attributes of the user object as a dictionary
            user_dict = tweet.user.__dict__
            #set the time zone of the tweet.date to UTC to ensure the datetime object is in a standardized format
            tweet_date = tweet.date.replace(tzinfo=timezone)
            #update_date=diff_date.replace(tzinfo=timezone)
            if start_date <= tweet_date.date()<=end_date:
                tweet_date_iso = tweet_date.isoformat()
            # convert the user_dict object into a JSON formatted string and calls the JSON serial function
            # if the object is not serialized by default json code,json.dump() method can't serialize the datetime object by default.
            user_json = json.dumps(user_dict, default=json_serial)
            tweets.append([query,tweet.url, tweet_date_iso, tweet.id, tweet.content, user_json,
                           tweet.replyCount, tweet.retweetCount, tweet.likeCount,
                           tweet.lang, tweet.source, tweet.conversationId])
    return tweets

c=0
submit_button=st.button('SUBMIT')
if st.session_state.get('button') != True:
    st.session_state['button'] = submit_button

if st.session_state['button'] == True:
    c = c + 1
    tweets = get_tweets()
    if tweets is not None:
        st.success('Submitted successfully!!!!!!')
        df = pd.DataFrame(tweets, columns=['KEY WORD', 'URL', 'DATE', 'ID', 'CONTENT', 'USER',
                                           'REPLY COUNT', 'RETWEET COUNT', 'LIKE COUNT',
                                           'LANGUANGE', 'SOURCE', 'CONVERSATION ID'])

        col1,col6 = st.columns([1,1])
        with col1:
            if st.button('View Data') and c>=1:
                st.dataframe(df)
                st.success('Scrapped Data Retrieved Successfully !!!!!!')

        with col6:
            if st.button('SAVE'):
                st.write('saving...')
                # converts the DataFrame to a list of dictionaries
                tweets_dict = df.to_dict('records')
                # Store the data in MongoDB
                for tweet in tweets_dict:
                    if collection.find_one({"ID": tweet["ID"]}) is None:
                        collection.insert_one(tweet)
                    else:
                        collection.update_one({'ID': tweet['ID']}, {"$set": tweet}, upsert=False)
                st.success('Saved Successfully into Mongo DB!!!!!!')

        col1,col6 = st.columns([1,1])
        with col1:
            # Download as CSV format
            if st.button('Download in CSV'):
                current_date = dt.now().strftime("%Y_%m_%d")
                file_name = query + '_' + str(dt.today().date())  + '.csv'
                df.to_csv(file_name, index=False)
                st.success('Data downloaded successfully in csv format!!!!!!')

        with col6:
            # Download as JSON format
            if st.button('Download in JSON'):
                current_date = dt.now().strftime("%Y_%m_%d")
                file_name = query + '_' + str(dt.today().date())  + '.json'
                df.to_json(file_name, orient='index')
                st.success('Data downloaded successfully in json format!!!!!!')

