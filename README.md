# Rio-2016-Twitter-Tweet-Data-Analysis-using-Hadoop-Map-Reduce
Analysing Twitter data based on Hashtags used during Rio Olympics 2016.


DATASET:
The folder /data/olympictweets2016rio contains a large collection of Twitter messages captured during the Rio 2016 Olympics period. The messages were collected by connecting to Twitter Streaming API, and filtering only messages directly related to the Olympic Games (by requesting they include a related hashtag such as #Rio2016 or #rioolympics ).

A MapReduce job that points to that folder as input will receive as Text value the whole contents for a single entry (tweet), in the format:
epoch_time;tweetId;tweet(including #hashtags);device
epoch_time provides the time the message was published, expressed in miliseconds since 01-01-1970.
tweetId is a unique identifier per message.
tweet includes the message itself. hashtags are in the tweets, identified by the hash symbol.
device provides additional meta-information, including the type of device/app used to submit the message, and a shortened url to access the message.
An example entry for the dataset is: 
1469453965000;757570957502394369;Over 30 million women footballers in the world. Most of us would trade places with this lot for #Rio2016  https://t.co/Mu5miVJAWx;<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

ALso included in the project is the IntIntPair object, a Writable class that can store a pair of int values in a single Writable. We do not enforce the use of that class as part of some of your solutions, but it might become useful at some point during the project execution.



