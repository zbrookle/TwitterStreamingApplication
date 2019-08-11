package com.ZachApp.app;

// JavaSwing packages
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.*;
import javafx.scene.layout.GridPane;
import javafx.geometry.Rectangle2D;
import javafx.geometry.Orientation;
import javafx.scene.chart.*;
import javafx.scene.text.Text;
import java.util.concurrent.atomic.AtomicReference;

// Twitter packages
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.GeoLocation.*;

// Java packages
import java.util.*;

// CSV Output
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;

// Spark
import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalog.Column;

class TwitterDataStream {
  TwitterStream twitterStream;
  StatusListener listener;
  BufferedWriter writer;
  CSVPrinter csvPrinter;
  int i;
  int tweetCount;

  TwitterDataStream(final GridPane twitterFeedVisual) {
    // Initialize the stream instance
    twitterStream = new TwitterStreamFactory().getInstance();
    i = 0;
    tweetCount = 0;

    // Set up the listener
    listener = new StatusListener() {
      // Create place to store data
      public void onStatus(final Status status) {
          // Arrange data in a list
          ArrayList row;
          try {
            row = new ArrayList() {{
                                    add(status.getUser().getName());
                                    add(status.getUser().getId());
                                    add(status.getCreatedAt());
                                    add(status.getDisplayTextRangeStart());
                                    add(status.getDisplayTextRangeEnd());
                                    add(status.getFavoriteCount());
                                    add(status.getLang());
                                    add(status.getPlace().getCountry());
                                    add(status.getPlace().getGeometryCoordinates().toString());
                                    add(status.getRetweetCount());
                                    add(status.getText());
                                    add(status.isRetweet());
                                  }};
          } catch (NullPointerException e) {
            row = new ArrayList() {{
                                    add(status.getUser().getName());
                                    add(status.getUser().getId());
                                    add(status.getCreatedAt());
                                    add(status.getDisplayTextRangeStart());
                                    add(status.getDisplayTextRangeEnd());
                                    add(status.getFavoriteCount());
                                    add(status.getLang());
                                    add("");
                                    add("");
                                    add(status.getRetweetCount());
                                    add(status.getText());
                                    //add(status.getWithheldInCountries());
                                    add(status.isRetweet());
                                  }};
          }

          // Add the tweet to the interface
          Text tweet_text = new Text(status.getText());
          twitterFeedVisual.addRow(tweetCount, tweet_text);

          // Create the csv file to write to
          try {
            writer = Files.newBufferedWriter(Paths.get("stream/data-" + Integer.toString(i) + ".csv"));
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                            .withHeader("UserName",
                                        "UserID",
                                        "created_at",
                                        "TextRangeStart",
                                        "TextRangeEnd",
                                        "FavoriteCount",
                                        "Language",
                                        "Place",
                                        "Coordinates",
                                        "RetweetCount",
                                        "Text",
                                        "isRetweet"));
            i++;
          } catch (IOException e) {
            System.out.println("ERROR: Problem creating data file");
          }

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

class SparkStreamer {
  private SparkSession spark;
  private StructType schema;
  private Dataset csvDF;

  SparkStreamer () throws StreamingQueryException {
    // Initialize instance of the spark app
    Builder builder = new Builder();
    spark = builder.master("local").appName("TwitterStream").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    // Create Schema for data
    schema = new StructType(new StructField[] {
      new StructField("UserID", DataTypes.LongType, false, Metadata.empty()),
      new StructField("created_at", DataTypes.StringType, false, Metadata.empty()),
      new StructField("TextRangeStart", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("TextRangeEnd", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("FavoriteCount", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("Language", DataTypes.StringType, false, Metadata.empty()),
      new StructField("Place", DataTypes.StringType, false, Metadata.empty()),
      new StructField("Coordinates", DataTypes.StringType, false, Metadata.empty()),
      new StructField("RetweetCount", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("Text", DataTypes.StringType, false, Metadata.empty()),
      // new StructField("WithheldInCountries", DataTypes.ArrayType(DataTypes.StringType, false), false, Metadata.empty()),
      new StructField("isRetweet", DataTypes.BooleanType, false, Metadata.empty())
    });

    // Define where to read the twitter data from
    csvDF = spark.readStream().option("sep", ",").schema(schema).csv("stream/data*.csv");
    try {
      csvDF.createTempView("TWEET_DATA");
    } catch(AnalysisException e) {
      System.out.println("ERROR: Problem creating temp view");
    }

    // Dataset filtered = csvDF.agg(max(Column("RetweetCount")));
    Dataset filtered = spark.sql("select max(TextRangeEnd) from TWEET_DATA");

    StreamingQuery query;
    query = filtered.writeStream()
      .outputMode("complete")
      .option("checkpointLocation", "checkpoint/")
      .format("console")
      .start();

    //System.out.println(query.status());

    query.awaitTermination();
  }
}

// public class Model extends Thread {
//   private IntegerProperty intProperty;
//
//   public Model() {
//     intProperty = new SimpleIntegerProperty(this, "int", 0);
//     setDaemon(true);
//   }
//
//   public int getInt() {
//     return intProperty.get();
//   }
//
//   public IntegerProperty intProperty() {
//     return intProperty;
//   }
//
//   @Override
//   public void run() {
//     while (true) {
//       intProperty.set(intProperty.get() + 1);
//     }
//   }
// }

public class App extends Application{
  // public static void main(String[] args) {

  //
  //   // Delete files from stream when done
  //
  //   // Start Application
  //   new Hello_World();
  //
  // }
  private SparkStreamer sparkTweets;
  GridPane twitterFeedPane;

  @Override
  public void start(Stage primaryStage) throws Exception {
    // Create a root pane
    GridPane root = new GridPane();

    // Interface Column
    GridPane inputPane = new GridPane();
    // Get screen bounds
    Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
    double xPos = screenBounds.getMinX();
    double yPos = screenBounds.getMinY();
    double screenWidth = screenBounds.getWidth();
    double screenHeight = screenBounds.getHeight();

    Label keywordsLabel = new Label("Please enter keywords");
    inputPane.addRow(0, keywordsLabel); // Add instructions

    final TextField keywordsInput = new TextField();
    inputPane.addRow(1, keywordsInput);  // Add text field for key words

    // Set up button that will Initialize the twitter feed analysis
    Button startFeed = new Button("Start Twitter Feed");
    startFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(ActionEvent arg0) {
          TwitterDataStream myStream = new TwitterDataStream(twitterFeedPane);
          String[] words = new String[]{keywordsInput.getText()};
          myStream.StreamData(words);
          try {
            sparkTweets = new SparkStreamer();
          } catch (StreamingQueryException e) {
            System.out.println("Stream exception!");
          }
        }
    });
    inputPane.addRow(2, startFeed); // Add the start button

    // Chart column
    GridPane chartPane = new GridPane();
    PieChart piechart = new PieChart(); // Create Pie chart
    chartPane.addRow(0, piechart); // Add pie chart
    // StackedBarChart barchart = new StackedBarChart(22);
    // chartPane.addRow(1, barchart);

    // Twitter feed column
    twitterFeedPane = new GridPane();
    twitterFeedPane.setMinWidth(screenWidth/3);
    twitterFeedPane.setMaxWidth(screenWidth/3);
    twitterFeedPane.setMinHeight(screenHeight);
    twitterFeedPane.setMaxHeight(screenHeight);
    ScrollBar feedScrollBar = new ScrollBar();
    feedScrollBar.setOrientation(Orientation.VERTICAL);
    twitterFeedPane.getChildren().add(feedScrollBar);

    // Add panes to the root
    root.addColumn(0, inputPane);
    root.addColumn(1, chartPane);
    root.addColumn(2, twitterFeedPane);

    // Set the scene
    Scene scene = new Scene(root, screenWidth, screenHeight);
    primaryStage.setScene(scene);

    // Set stage to full screen
    primaryStage.setX(xPos);
    primaryStage.setY(yPos);
    primaryStage.setWidth(screenWidth);
    primaryStage.setHeight(screenHeight);

    primaryStage.setTitle("Twitter Stream"); // Set the title
    primaryStage.show(); // Show app
  }

  public static void main (String[] args)
  {
      launch(args);
  }
}
