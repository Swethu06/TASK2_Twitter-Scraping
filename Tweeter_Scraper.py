import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import json
from datetime import datetime
import pytz
from streamlit import session_state

with st.form(key='Twitter_form'):
    st.title("Twitter Scrapper")
    query=st.text_input("Enter the Keyword or Hashtag to be searched :", value="")
    query = query.strip()
    date_range = st.selectbox("Choose the date range :",["Last hour","Last day","Last week","Last month","Last 100 days","Last year"])
    limit=st.slider("Choose the limit to be searched :",1,10000,step=30)
    submit_button=st.form_submit_button(label='SUBMIT')
    tweets = []

    session_state.counter = session_state.get("counter", 0)


    def json_serial(obj):
        # check if the passed object(not serializable by default json code) obj is an instance of the datetime class.
        if isinstance(obj, (datetime)):
            return obj.isoformat()
        if isinstance(obj, sntwitter.TextLink):
            return obj.url
        if isinstance(obj, sntwitter.UserLabel):
            return obj.user_label
        raise TypeError("Type %s not serializable" % type(obj))


    # Function to convert the date range string to seconds
    def get_seconds(date_range):
        if date_range == "Last hour":
            return 3600
        elif date_range == "Last day":
            return 86400
        elif date_range == "Last week":
            return 604800
        elif date_range == "Last month":
            return 2592000
        elif date_range == "Last 100 days":
            return 8640000
        elif date_range == "Last year":
            return 31536000


    def get_tweets():
        tweets = []
        seconds = get_seconds(date_range)
        timezone = pytz.timezone("UTC")
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
                tweet_date = tweet.date.replace(tzinfo=timezone)
                current_time = datetime.now().replace(tzinfo=timezone)
                if current_time and tweet_date:
                    diff_time = current_time - tweet_date
                    if diff_time:
                        if (current_time - tweet_date).total_seconds() <= seconds:
                            tweet_date_iso = tweet_date.isoformat()

                            # convert the user_dict object into a JSON formatted string and calls the JSON serial function
                            # if the object is not serialized by default json code)
                            # json.dump() method can't serialize the datetime object by default.
                            user_json = json.dumps(user_dict, default=json_serial)
                            tweets.append([tweet.url, tweet_date_iso, tweet.id, tweet.content, user_json,
                                           tweet.replyCount, tweet.retweetCount, tweet.likeCount,
                                           tweet.lang, tweet.source, tweet.conversationId])
        return tweets


    if submit_button:
        session_state.counter += 1
        st.success('Submitted successfully!!!!!!')
        tweets = get_tweets()
        if tweets:
            df = pd.DataFrame(tweets, columns=['URL', 'DATE', 'ID', 'CONTENT', 'USER',
                                               'REPLY COUNT', 'RETWEET COUNT', 'LIKE COUNT',
                                               'LANGUANGE', 'SOURCE', 'CONVERSATION ID'])
            st.dataframe(df)
            st.success('Scrapped Data Retrieved Successfully !!!!!!')

            if st.checkbox("Save Data"):
              if session_state.counter >= 1:
                st.write("Saving...")
                # Store the data in MongoDB
                client = MongoClient()
                db = client["twitter_db"]
                collection = db["twitter_data"]
                collection.insert_many(df.to_dict("records"))
                st.success('Saved Successfully into Mongo DB!!!!!!')

            select_option1 = st.radio("Choose the desired format to be downloaded ", ('','CSV', 'JSON'))
                # if st.write("Download the scrapped data", st.radio("Download")):
            if select_option1 == 'CSV':
                        # if st.button("Download as CSV format"):
                        current_time = datetime.now().strftime("%Y_%m_%d")
                        file_name = query + '_' + current_time + '.csv'
                        save_path = st.text_input("Enter the path to save the file:", type="csv")
                        if save_path:
                            full_path = save_path + "/" + file_name
                            df.to_csv(full_path,index=False)
                            st.success('Data downloaded successfully in csv format!!!!!!')
                            st.markdown("```")
                            st.write(csv)
                            st.markdown("```")
            elif select_option1 == 'JSON':
                        # if st.button("Download as JSON format"):
                        current_time = datetime.now().strftime("%Y-%m-%d")
                        file_name = query + "_" + current_time + ".json"
                        save_path = st.file_uploader("Select a location to save the file", type="json")
                        if save_path:
                            full_path = save_path + "/" + file_name
                            df.to_json(full_path)
                            st.success('Data downloaded successfully in json format!!!!!!')
                            st.markdown("```")
                            st.write(json)
                            st.markdown("```")
        else:
            st.warning("Scrape the data before downloading")
    # if submit_button:
    #     session_state.counter += 1
    #     st.success('Submitted successfully!!!!!!')
    #     tweets = get_tweets()
    #     if tweets:
    #         df = pd.DataFrame(tweets, columns=['URL', 'DATE', 'ID', 'CONTENT', 'USER',
    #                                            'REPLY COUNT', 'RETWEET COUNT', 'LIKE COUNT',
    #                                            'LANGUANGE', 'SOURCE', 'CONVERSATION ID'])
    #         # Store the data in MongoDB
    #         client = MongoClient()
    #         db = client.twitter_db
    #         tweets_dict = df.to_dict('records')
    #         # Show scraped tweets in dataframe
    #         if st.checkbox('Show Data'):
    #             st.dataframe(df)
    #             st.success("Displays the scrapped tweet based on the given input criteria")
    #         if st.checkbox('Save Data'):
    #             db.tweets.insert_many(tweets_dict)
    #             st.success("Scraped tweets has been stored in MongoDB")
    #         # Download data in csv and json format
    #         if st.checkbox('Download'):
    #             csv = df.to_csv()
    #             json = df.to_json()
    #             st.write("Download as CSV : ", st.link('Download', csv))
    #             st.write("Download as JSON : ", st.link('Download', json))
