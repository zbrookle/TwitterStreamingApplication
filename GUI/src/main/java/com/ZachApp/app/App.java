package com.ZachApp.app;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

class TwitterData {
  twitter4j.TwitterFactory twitterFactory;
  twitter4j.Twitter twitter;

  TwitterData() {
    // Get instance of twitter
    twitterFactory = new TwitterFactory();
    twitter = twitterFactory.getInstance();
  }

  public void getTweets(String input_query) {
    // Query data
    Query query = new Query(input_query);
    try {
      QueryResult result = twitter.search(query);

      // Display data
      int tweetcount = result.getCount();
      System.out.println(tweetcount);
      for (Status status : result.getTweets()) {
          System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
      }
    } catch (TwitterException name) {
        System.out.println("You don't have internet connection.");
    }
  }
}

public class App {
  public static void main(String[] args) {
    TwitterData myTwitter = new TwitterData();
    myTwitter.getTweets("Harry Potter");
  }
}
