package com.ZachApp.app;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.*;

class TwitterDataStream {
  TwitterStream twitterStream;
  StatusListener listener;

  TwitterDataStream() {
    // Initialize the stream instance
    twitterStream = new TwitterStreamFactory().getInstance();

    // Set up the listener
    listener = new StatusListener () {
      // Create place to store data
      ArrayList<ArrayList> tweetData = new ArrayList<ArrayList>();
      String[] HEADERS = {"UserID",
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

      public void onStatus(final Status status) {

          // Arrange data in a list
          ArrayList row = new ArrayList() {{
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
          // Write data out to a CSV file with the appropriate headers

          tweetData.add(row);
          System.out.println(row);
      }
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
      public void onException(Exception ex) {
          ex.printStackTrace();
      }
      public void onStallWarning(StallWarning arg0) {}
      public void onScrubGeo(long arg0,long arg1) {}
    };

    // Add the listener
    twitterStream.addListener(listener);
  }

  public void StreamData(String[] keywords) {
    FilterQuery tweetFilterQuery = new FilterQuery();
    tweetFilterQuery.track(keywords);

    twitterStream.filter(tweetFilterQuery);
  }
}

public class App {
  public static void main(String[] args) {
    // TwitterData myTwitter = new TwitterData();
    TwitterDataStream myStream = new TwitterDataStream();
    String[] words = new String[]{"Harry Potter", "Hunter X Hunter"};
    myStream.StreamData(words);
  }
}
