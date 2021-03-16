#import libraries
import tweepy
import pandas as pd
import datetime
import csv
import smtplib
import string
import smtplib
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart
from datetime import timedelta
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
import os

#import Twitter API authenticiation info
twitter_keys = {
    'consumer_key': 'INSERT YOURS HERE',
    'consumer_secret': 'INSERT YOURS HERE',
    'access_token_key': 'INSERT YOURS HERE',
    'access_token_secret': 'INSERT YOURS HERE'
}

#Setup access to API
auth = tweepy.OAuthHandler(twitter_keys['consumer_key'],
                           twitter_keys['consumer_secret'])
auth.set_access_token(twitter_keys['access_token_key'],
                      twitter_keys['access_token_secret'])
api = tweepy.API(auth)

list_id = #insert you list id here

#define the function that will collect the maximum number of most recent tweets — 3,200, 
#analyze the tweets for keywords, append the individual assigned to the state, 
#and save the file based on the date for archival

def get_all_tweets(list_id):

    #initialize a list to hold all the tweepy Tweets
    alltweets = []

    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.list_timeline(list_id= "YOUR LIST ID",
                                   include_rts=False,
                                   count=200,
                                   result_type='recent',
                                   truncated=False,
                                   include_entites=True)

    #save most recent tweets
    alltweets.extend(new_tweets)

    #save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print("getting tweets")

        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.list_timeline(list_id=list_id,
                                       count=200,
                                       max_id=oldest)

        #save most recent tweets
        alltweets.extend(new_tweets)

        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        print(f"...{len(alltweets)} tweets downloaded so far")

    #transform the tweepy tweets into a 2D array 
    outtweets = [[
        tweet.created_at, tweet.user.screen_name, tweet.id_str, tweet.text,
        tweet.user.location
    ] for tweet in alltweets]
    
    #transform the 2D array into a pandas dataframe
    tweet_text = pd.DataFrame(data = outtweets, columns = ["Tweet Date", "User", "Tweet ID", 'Text', 'Location'])
    tweet_text['Tweet Date'] = tweet_text['Tweet Date'].astype(str)
    
    #create a column to note the day
    tweet_text['Date of Update'] = datetime.date.today()

    #Create separate dataframes while will 'house' tweets based on keyword
    reopening = tweet_text
    effective = tweet_text
    hybrid = tweet_text
    virtual = tweet_text
    in_person = tweet_text
    open_ = tweet_text
    close = tweet_text
    remote = tweet_text
    _return = tweet_text

    #Filter data based on keyword and assign to appropriate dataframe
    reopening = tweet_text.apply(
        lambda row: row.map(str).str.contains('reopen', case=False).any(), axis=1)
    effective = tweet_text.apply(
        lambda row: row.map(str).str.contains('effective', case=False).any(),
       axis=1)
    hybrid = tweet_text.apply(
        lambda row: row.map(str).str.contains('hybrid', case=False).any(), axis=1)
    virtual = tweet_text.apply(
        lambda row: row.map(str).str.contains('virtual', case=False).any(), axis=1)
    in_person = tweet_text.apply(
        lambda row: row.map(str).str.contains('in-person', case=False).any(),
     axis=1)
    open_ = tweet_text.apply(
        lambda row: row.map(str).str.contains('open', case=False).any(), axis=1)
    close = tweet_text.apply(
        lambda row: row.map(str).str.contains('close', case=False).any(), axis=1)
    remote = tweet_text.apply(
        lambda row: row.map(str).str.contains('remote', case=False).any(), axis=1)
    _return = tweet_text.apply(
        lambda row: row.map(str).str.contains('return', case=False).any(), axis=1)

    #Create keyword values for 'Keyword' column to understand at a glance what the Tweet is about
    df_reopening = tweet_text.loc[reopening]
    df_reopening['Keyword'] = 'Reopening'

    df_effective = tweet_text.loc[effective]
    df_effective['Keyword'] = 'Effective'

    df_hybrid = tweet_text.loc[hybrid]
    df_hybrid['Keyword'] = 'Hybrid'

    df_virtual = tweet_text.loc[virtual]
    df_virtual['Keyword'] = 'Virtual'

    df_in_person = tweet_text.loc[in_person]
    df_in_person['Keyword'] = 'In-Person'

    df_open_ = tweet_text.loc[open_]
    df_open_['Keyword'] = 'Open'

    df_close = tweet_text.loc[close]
    df_close['Keyword'] = 'Close'
    
    df_remote = tweet_text.loc[remote]
    df_remote['Keyword'] = 'Remote'
    
    df_return = tweet_text.loc[_return]
    df_return['Keyword'] = 'Return'

    #Append the data frames together
    df_concat_tweetes = pd.concat([
        df_reopening, df_effective, df_hybrid, df_virtual, df_in_person, df_open_,
       df_close, df_remote, df_return
    ])

    #split tweet.json.location to isolate the state
    split_df = df_concat_tweetes["Location"].str.split(',', expand=True)
    split_df.columns = [f"Location_{id_}" for id_ in range(len(split_df.columns))]
    df_concat_tweetes = pd.merge(df_concat_tweetes,
                             split_df,
                             how="left",
                             left_index=True,
                             right_index=True)

    #drop extra columns, rename tweet.json.location.state 'Location_1' to 'Tweet_Location',
    df_concat_tweetes = df_concat_tweetes.drop(columns=['Location_0'])
    df_concat_tweetes = df_concat_tweetes.rename(
        columns={'Location_1': 'Tweet_Location'})
    df_concat_tweetes = df_concat_tweetes.drop(columns=['Location'])

    #sort values by 'Tweet Date'
    df_concat_tweetes.sort_values(by='Tweet Date')

    #slice off extra space in the 'Location' column
    df_concat_tweetes["Tweet Date"] = df_concat_tweetes["Tweet Date"].str.slice(
       start=None, stop=10)
    df_concat_tweetes["Tweet_Location"] = df_concat_tweetes[
    "Tweet_Location"].str.slice(start=1, stop=None)

    #drop duplicaets
    df_concat_tweetes = df_concat_tweetes.drop_duplicates(subset=['Text'], keep='first')

    #limit the dataframe to tweets from the current date
    df_concat_tweets = df_concat_tweetes.loc[df_concat_tweetes['Tweet Date'].isin([{datetime.date.today()}])]

    #read in file containing mapping of individuals 
    X_coverage = pd.read_excel('"YOUR FILE.xlsx')

    #merge dataframe with coverage file on Twitter handles 
    df_concat_tweetes = pd.merge(df_concat_tweetes,
                                 X_coverage,
                                 how='left',
                                 left_on=['User'],
                                 right_on=['Twitter Handle'])

    #drop the duplicate column with Twitter handle info
    df_concat_tweetes = df_concat_tweetes.drop(columns=['User'])

    #drop Tweet Location
    df_concat_tweetes = df_concat_tweetes.drop(columns=['Tweet_Location'])
    
    #Reorder data frame alphabetically to ensure later import matches with new data and doesn't create unncessary columns
    #this dataframe — df_concat_tweets — holds all the new Tweets of interest to us
    df_concat_tweetes = df_concat_tweetes[[
        'Date of Update', 'Tweet Date','Keyword', 'Twitter Handle', 'Text', 'District',  'State', 'Policy Coverage', 'Email' 
    ] + []]
    
    df_concat_tweetes.to_csv('district_tweets.csv')
    
    #save the pandas dataframe to a numpy array
    array = df_concat_tweetes.to_numpy()
    
    #create person-specific dataframes
    df_kory = df_concat_tweetes.loc[df_concat_tweetes['X Coverage'].isin(['Kory'])]
  
    #email login info here
    login = "" # paste your login 
    password = "" # paste your password 
    sender_email = "" #paste your email
    
    #send an email
    receiver_email = "YOUR TO: EMAIL"
    subject = "COVID: All New District Tweets — " + datetime.date.today().strftime("%m/%d/%Y") 
    body = "COVID: All New District Tweets for " + datetime.date.today().strftime("%m/%d/%Y")

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    filename = "district_tweets.csv"  # In same directory as script

    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

    print('Sent email!')
    
    #delete the current csv file 
    os.remove('/Users/"PATH HERE"/district_tweets.csv')
    print("Today's district tweet file has been deleted.")
    
    #write the csv, include {datetime} function to automatically save the file with today's date for archival
    with open(f'/Users/"USER PATH"/District_Tweets/{datetime.date.today()}_school_district_tweets.csv', 'w', encoding='utf-8') as t:
        writer = csv.writer(t)
        writer.writerow(['Date of Update', 'Tweet Date','Keyword', 'Twitter Handle', 'Text', 'District',  'State', 'Policy Coverage', 'Email'])
        writer.writerows(array)
        print('File was written to the desktop.')
        
    pass
    
if __name__ == '__main__':
    #pass in the username of the account you want to download
    get_all_tweets("YOUR LIST ID HERE")
