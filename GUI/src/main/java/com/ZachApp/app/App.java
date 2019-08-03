package com.ZachApp.app;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.*;

class TwitterData {
  TwitterFactory twitterFactory;
  Twitter twitter;
  public String[] HEADERS  = {"UserID",
                              "created_at",
                              "TextRangeStart",
                              "TextRangeEnd",
                              "FavoriteCount",
                              "Language",
                              "Place",
                              "RetweetCount",
                              "Text",
                              "WithheldInCountries",
                              "isRetweet"};

  TwitterData() {
    // Get instance of twitter
    twitterFactory = new TwitterFactory();
    twitter = twitterFactory.getInstance();
  }

  public void getTweets(String input_query) {
    // Create list to place tweets into
    ArrayList<ArrayList> tweetData = new ArrayList<ArrayList>();

    // Query data
    Query query = new Query(input_query);
    try {
      QueryResult result = twitter.search(query);

      ArrayList row;
      for (final Status status : result.getTweets()) {
          // Get the data
          row = new ArrayList() {{
                                  add(status.getUser().getId());
                                  add(status.getCreatedAt());
                                  add(status.getCreatedAt());
                                  add(status.getDisplayTextRangeStart());
                                  add(status.getDisplayTextRangeEnd());
                                  add(status.getFavoriteCount());
                                  add(status.getLang());
                                  add(status.getPlace());
                                  add(status.getRetweetCount());
                                  add(status.getText());
                                  add(status.getWithheldInCountries());
                                  add(status.isRetweet());
                                }};
          tweetData.add(row);
      }
    } catch (TwitterException name) {
        System.out.println("You don't have internet connection.");
    }
    System.out.println(tweetData);
  }
}

public class App {
  public static void main(String[] args) {
    TwitterData myTwitter = new TwitterData();
    myTwitter.getTweets("Harry Potter");
  }
}
