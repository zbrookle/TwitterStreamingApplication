package com.ZachApp.app;

// Threading Packages
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

// JavaFX packages
import javafx.application.Application;
import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Label;
import javafx.scene.control.Control;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.scene.layout.GridPane;
import javafx.scene.chart.PieChart;
import javafx.scene.layout.Region;
import javafx.geometry.Rectangle2D;
import javafx.beans.property.StringProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.geometry.Pos;

// Twitter packages
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StallWarning;

// Java packages
import java.util.ArrayList;

// CSV Output
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.BufferedWriter;
import java.io.IOException;

// Spark
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.streaming.StreamingQueryManager;
import org.apache.spark.sql.streaming.StreamingQueryListener;

/**
 * TwitterDataStream connects to the twitter api and writes tweet data to csv
 * files and to the UI.
 */
class TwitterDataStream {
  /**
   * The twitter stream instance.
   */
  private TwitterStream twitterStream;

  /**
    * Gets passed to the csv printer to output data to files.
    */
  private BufferedWriter writer;

  /**
    * Responsible for properly outputting data to csv files.
    */
  private CSVPrinter csvPrinter;

  /**
    * Tracks the partition number.
    */
  private int partition;

  /**
    * Constructor for the datastream class.
    * @param currentTweetText is text of the current tweet
    * @param endFeed is an atomic boolean that receives from the UI when to
    * terminate the stream
    */
  TwitterDataStream(final StringProperty currentTweetText, final AtomicBoolean endFeed) {
    // Initialize the stream instance
    twitterStream = new TwitterStreamFactory().getInstance();
    partition = 0;

    // Set up the listener
    StatusListener listener = new StatusListener() {
      // Create place to store data
      public void onStatus(final Status status) {
          if (endFeed.get()) {
            twitterStream.shutdown();
            System.out.println("Stopping twitter!");
          }

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
                                    add(status.isRetweet());
                                  }};
          }

          // Change the text of the current tweet
          currentTweetText.set(status.getUser().getName() + ":" + status.getText());

          // Create the csv file to write to
          try {
            writer = Files.newBufferedWriter(Paths.get("stream/data-" + Integer.toString(partition) + ".csv"));
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
            partition++;
          } catch (IOException e) {
            System.out.println("ERROR: Problem creating data file");
          }

          // Write data out to a CSV file with the appropriate headers
          try {
            csvPrinter.printRecord(row);
            csvPrinter.flush();
          } catch (IOException e) {
            System.out.println("ERROR: Could not write to file");
            System.out.println(row);
          }
      }

      // All these methods needed to be overridden according to the api
      public void onDeletionNotice(final StatusDeletionNotice statusDeletionNotice) { }
      public void onTrackLimitationNotice(final int numberOfLimitedStatuses) { }
      public void onException(final Exception ex) {
          ex.printStackTrace();
      }
      public void onStallWarning(final StallWarning arg0) { }
      public void onScrubGeo(final long arg0, final long arg1) { }
    };

    // Add the listener
    twitterStream.addListener(listener);
  }

  /**
    * Method that begins the streaming process.
    * @param keywords is an array of key words to look for in tweets
    */
  public void streamData(final String[] keywords) {
    // Create a new filter instance
    FilterQuery tweetFilterQuery = new FilterQuery();

    // Add the keywords to the query
    tweetFilterQuery.track(keywords);

    // Start the stream
    twitterStream.filter(tweetFilterQuery);
  }

  public void stopStreaming() {
    twitterStream.shutdown();
  }
}

/**
 * The SparkStreamer handles all aggregations and processing of the twitter
 * data. It reads from the csv's output by the TwitterDataStream.
 */
class SparkStreamer {

  /**
   * Instance of the spakr session.
   */
  private SparkSession spark;

  /**
   * Defines the structure of the data that will be processed by spark.
   */
  private StructType schema;

  /**
   * Dataset to store the data output of the twitter stream.
   */
  private Dataset csvDF;

  /**
    * Instance of the query that is currently streaming.
    */
  private StreamingQuery query;

  /**
    * Constructor method for the spark stream.
    * @throws StreamingQueryException handles errors that may occur during
    * streaming
    * @param endFeed is an atomic boolean that receives from the UI when to
    * terminate the stream
    */
  SparkStreamer(final AtomicBoolean endFeed) throws StreamingQueryException {
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
      new StructField("isRetweet", DataTypes.BooleanType, false, Metadata.empty())
    });

    // Define where to read the twitter data from
    csvDF = spark.readStream().option("sep", ",").schema(schema).csv("stream/data*.csv");
    try {
      csvDF.createTempView("TWEET_DATA");
    } catch (AnalysisException e) {
      System.out.println("ERROR: Problem creating temp view");
    }

    // Dataset filtered = csvDF.agg(max(Column("RetweetCount")));
    Dataset filtered = spark.sql("select max(TextRangeEnd) from TWEET_DATA");

    StreamingQueryManager manager = new StreamingQueryManager(spark);
    manager.addListener(new StreamingQueryListener() {
      @Override
      public void onQueryStarted(QueryStartedEvent event) {

      }

      @Override
      public void onQueryProgress(QueryProgressEvent event) {
        if (endFeed.get()) {
          query.stop();
          spark.stop();
          System.out.println("Stopping spark!");
        }
      }

      @Override
      public void onQueryTerminated(QueryTerminatedEvent event) {

      }
    });

    query = filtered.writeStream()
      .outputMode("complete")
      .option("checkpointLocation", "checkpoint/")
      .format("console")
      .start();
  }
}

/**
 * App is the user interface and main class. It encompasses user input and
 * visuals such as graphs. It is implemented using JavaFX.
 */
public class App extends Application {
  /**
    * Keeps track of the number of tweets that have been added to the visual
    * feed.
    */
  private int tweetCount;

  private void setRegionSize(final Region region, double width, double height) {
    region.setPrefSize(width, height);
    region.setMinSize(Control.USE_PREF_SIZE, Control.USE_PREF_SIZE);
    region.setMaxSize(Control.USE_PREF_SIZE, Control.USE_PREF_SIZE);
  }

  @Override
  public void start(final Stage primaryStage) throws Exception {
    // Create a root pane
    GridPane root = new GridPane();

    // Create an AtomicReference with a string to pass tweets between front and back end
    final AtomicReference<String> tweetText = new AtomicReference("");

    // Create an atomic boolean to pass between front and backend to terminate the streaming instances
    final AtomicBoolean endPressed = new AtomicBoolean(false);

    // Get screen bounds
    Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
    final double xPos = screenBounds.getMinX();
    final double yPos = screenBounds.getMinY();
    final double screenWidth = screenBounds.getWidth();
    final double screenHeight = screenBounds.getHeight() - screenBounds.getHeight() / 100;
    final double columnWidth = screenWidth / 3;

    /* Chart column */
    GridPane chartPane = new GridPane();
    PieChart piechart = new PieChart(); // Create Pie chart
    chartPane.addRow(0, piechart); // Add pie chart
    // StackedBarChart barchart = new StackedBarChart(22);
    setRegionSize(chartPane, columnWidth, screenHeight);

    /* Twitter feed column */
    // Set content pane
    final GridPane twitterFeedPane = new GridPane();
    twitterFeedPane.setPrefSize(columnWidth, screenHeight);
    twitterFeedPane.setMinSize(Control.USE_PREF_SIZE, Control.USE_PREF_SIZE);
    twitterFeedPane.setMaxWidth(Control.USE_PREF_SIZE);
    twitterFeedPane.setMaxHeight(Region.USE_COMPUTED_SIZE);

    // TODO add in scroll pane going down when new tweet if scrolled down to bottom
    // Set scrolling pane
    ScrollPane feedScrollPane = new ScrollPane(twitterFeedPane);
    setRegionSize(feedScrollPane, columnWidth, screenHeight);
    feedScrollPane.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
    feedScrollPane.setVbarPolicy(ScrollPane.ScrollBarPolicy.AS_NEEDED);

    tweetCount = 0; // Variable to keep track of tweet row
    final Model model = new Model();
    model.tweetProperty().addListener(new ChangeListener<String>() {
      @Override
      public void changed(final ObservableValue<? extends String> observable,
          final String oldValue, final String newValue) {
        if (tweetText.getAndSet(newValue.toString()) != "") {
          Platform.runLater(new Runnable() {
            @Override
            public void run() {
              double tweetBoxHeight = screenHeight / 12;
              if (tweetCount > 10) {
                twitterFeedPane.setPrefHeight(twitterFeedPane.getPrefHeight() + tweetBoxHeight);
              }
              Label tweetTextVisual = new Label(newValue.toString());
              tweetTextVisual.setWrapText(true);
              tweetTextVisual.setPrefSize(columnWidth, tweetBoxHeight);
              tweetTextVisual.setMinWidth(Control.USE_PREF_SIZE);
              tweetTextVisual.setMaxWidth(Control.USE_PREF_SIZE);
              twitterFeedPane.addRow(tweetCount, tweetTextVisual);
              tweetCount++;
              String value = tweetText.getAndSet("");
            }
          });
        }

      }
    });

    // model.endFeedProperty().addListener(new ChangeListener<String>() {
    //   @Override
    //   public void changed(final ObservableValue<? extends Boolean> observable,
    //       final Boolean oldValue, final Boolean newValue) {
    //     if (endPressed.getAndSet(newValue) != false) {
    //       Platform.runLater(new Runnable() {
    //         @Override
    //         public void run() {
    //
    //         }
    //       });
    //     }
    //
    //   }
    // });

    /* Interface Column */
    double inputPaneVerticalSpacing = screenHeight / 100;
    VBox inputPane = new VBox(inputPaneVerticalSpacing);
    inputPane.setAlignment(Pos.BASELINE_RIGHT);

    final Label instructions = new Label("Please enter keywords");

    HBox keywordsPane = new HBox();
    keywordsPane.setAlignment(Pos.BASELINE_RIGHT);
    Label keywordsLabel = new Label("Keywords:  ");
    final TextField keywordsInput = new TextField("Harry Potter");
    keywordsPane.getChildren().addAll(keywordsLabel, keywordsInput);

    // Set up buttons that will initialize and end the twitter feed analysis
    final Button startFeed = new Button("Start Twitter Feed");
    final Button endFeed = new Button("End Twitter Feed");
    endFeed.setDisable(true);

    startFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(final ActionEvent arg0) {
          String[] words = new String[]{keywordsInput.getText()};
          startFeed.setDisable(true);
          endFeed.setDisable(false);
          instructions.setText("Now streaming Twitter data");
          model.setKeywords(words);
          model.start();
        }
    });

    // TODO this doesn't work so try using an atomic boolean to do it instead
    endFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(final ActionEvent arg0) {
          startFeed.setDisable(false);
          endFeed.setDisable(true);
          model.getEndFeed().getAndSet(true);
        }
    });
    setRegionSize(inputPane, columnWidth, screenHeight);
    inputPane.getChildren().addAll(instructions, keywordsPane, startFeed, endFeed);

    // TODO Still need to add button that will terminate the stream

    // TODO Still need to add in a menu for the top bar

    // Add panes to the root
    root.addColumn(0, inputPane);
    root.addColumn(1, chartPane);
    root.addColumn(2, feedScrollPane);

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

  /**
    * Model handles the interaction between Spark and the front end.
    */
  public class Model extends Thread {
    /**
      * Wrapper for the text of the tweet.
      */
    private StringProperty tweetProperty;

    /**
      * Atomic boolean that determines whether to end the threads.
      */
    private AtomicBoolean endFeed;

    /**
      * Variable to store the app's instance of the spark streamer.
      */
    private SparkStreamer sparkTweets;

    /**
      * Variable used to store the app's instance of the twitter data stream
      */
    private TwitterDataStream myStream;

    /**
      * An array of words that will be passed to twitter4j to get tweets.
      */
    private String[] keywords;

    /**
      * Constructor for model.
      */
    public Model() {
      tweetProperty = new SimpleStringProperty(this, "string", "");
      endFeed = new AtomicBoolean(false);
      setDaemon(true);
    }

    /**
      * Get method for the endFeed AtomicBoolean
      * @return endFeed AtomicBoolean
      */
    public AtomicBoolean getEndFeed() {
      return endFeed;
    }

    /**
      * Get method for string of stirngProperty.
      * @return tweetProperty string
      */
    public String getTweet() {
      return tweetProperty.get();
    }

    /**
      * Get method for tweetProperty.
      * @return tweetProperty
      */
    public StringProperty tweetProperty() {
      return tweetProperty;
    }

    /**
      * Method for setting the key words that will be given to twitter.
      * @param inputWords is the list of words that will go to twitter
      */
    public void setKeywords(final String[] inputWords) {
      keywords = inputWords;
    }

    @Override
    public void start() {
      endFeed.set(false);
      TwitterDataStream myStream = new TwitterDataStream(tweetProperty, endFeed);
      myStream.streamData(keywords);
      try {
        sparkTweets = new SparkStreamer(endFeed);
      } catch (StreamingQueryException e) {
        System.out.println("Stream exception!");
      }
    }
  }

  /**
    * Starts the program.
    * @param args is the args
    */
  public static void main(final String[] args) {
      launch(args);
  }
}
