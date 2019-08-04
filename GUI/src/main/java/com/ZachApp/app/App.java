package com.ZachApp.app;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;

class TwitterDataStream {
  TwitterStream twitterStream;
  StatusListener listener;
  BufferedWriter writer;
  CSVPrinter csvPrinter;

  TwitterDataStream() {
    // Initialize the stream instance
    twitterStream = new TwitterStreamFactory().getInstance();

    // Create the csv file to write to
    try {
      writer = Files.newBufferedWriter(Paths.get("data.csv"));
      csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                      .withHeader("UserID",
                                  "created_at",
                                  "TextRangeStart",
                                  "TextRangeEnd",
                                  "FavoriteCount",
                                  "Language",
                                  "Place",
                                  "RetweetCount",
                                  "Text",
                                  "WithheldInCountries",
                                  "isRetweet"));
    } catch (IOException e) {
      System.out.println("ERROR: Problem creating data file");
    }

    // Set up the listener
    listener = new StatusListener() {
      // Create place to store data
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
          try {
            csvPrinter.printRecord(row);//status.getUser().getId(), status.getCreatedAt());
            csvPrinter.flush();
          } catch (IOException e) {
            System.out.println("ERROR: Could not write to file");
            System.out.println(row);
          }
      }

      // All these methods needed to be overridden according to the api
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
    // Create a new filter instance
    FilterQuery tweetFilterQuery = new FilterQuery();

    // Add the keywords to the query
    tweetFilterQuery.track(keywords);

    // Start the stream
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
