package com.ZachApp.app;

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
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.scene.layout.GridPane;
import javafx.scene.chart.PieChart;
import javafx.scene.text.Text;
import javafx.geometry.Rectangle2D;
import javafx.geometry.Orientation;
import javafx.beans.property.StringProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import java.util.concurrent.atomic.AtomicReference;

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
import java.lang.reflect.Array;

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
    */
  TwitterDataStream(final StringProperty currentTweetText) {
    // Initialize the stream instance
    twitterStream = new TwitterStreamFactory().getInstance();
    partition = 0;

    // Set up the listener
    StatusListener listener = new StatusListener() {
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
                                    add(status.isRetweet());
                                  }};
          }

          // Add the tweet to the interface
          // TODO change the value of the tweettext atomic AtomicReference
          currentTweetText.set(status.getText());

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
    * Constructor method for the spark stream.
    * @throws StreamingQueryException handles errors that may occur during
    * streaming
    */
  SparkStreamer() throws StreamingQueryException {
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

    StreamingQuery query;
    query = filtered.writeStream()
      .outputMode("complete")
      .option("checkpointLocation", "checkpoint/")
      .format("console")
      .start();

    //query.awaitTermination();
  }
}

/**
 * App is the user interface and main class. It encompasses user input and
 * visuals such as graphs. It is implemented using JavaFX.
 */
public class App extends Application {
  /**
    * Variable to store the app's twitter feed visual.
    */
  GridPane twitterFeedPane;

  int tweetCount;

  @Override
  public void start(final Stage primaryStage) throws Exception {
    // Create a root pane
    GridPane root = new GridPane();

    // Create an AtomicReference with a string to pass tweets between front and back end
    final AtomicReference<String> tweetText = new AtomicReference("");

    // Get screen bounds
    Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
    double xPos = screenBounds.getMinX();
    double yPos = screenBounds.getMinY();
    double screenWidth = screenBounds.getWidth();
    double screenHeight = screenBounds.getHeight();

    // Chart column
    GridPane chartPane = new GridPane();
    PieChart piechart = new PieChart(); // Create Pie chart
    chartPane.addRow(0, piechart); // Add pie chart
    // StackedBarChart barchart = new StackedBarChart(22);
    // chartPane.addRow(1, barchart);

    // Twitter feed column
    twitterFeedPane = new GridPane();
    twitterFeedPane.setMinSize(screenWidth / 3, screenHeight);
    twitterFeedPane.setMaxSize(screenWidth / 3, screenHeight);
    tweetCount = 0; // Variable to keep track of tweet row
    final Model model = new Model();
    model.stringProperty().addListener(new ChangeListener<String>() {
      @Override
      public void changed(final ObservableValue<? extends String> observable,
          final String oldValue, final String newValue) {
        if (tweetText.getAndSet(newValue.toString()) != "") {
          Platform.runLater(new Runnable() {
            @Override
            public void run() {
              Label tweetTextVisual = new Label(newValue.toString());
              Label.setWrapText(true);
              twitterFeedPane.addRow(tweetCount, tweetTextVisual);
              tweetCount++;
              String value = tweetText.getAndSet("");
            }
          });
        }

      }
    });
    anchorPane.setPrefSize(screenWidth/3, screenHeight)
    ScrollPane feedScrollPane = new ScrollPane(twitterFeedPane);
    feedScrollPane.setPrefViewportWidth(screenWidth / 3);
    feedScrollPane.setPrefViewportHeight(screenHeight);

    // Interface Column
    GridPane inputPane = new GridPane();

    Label keywordsLabel = new Label("Please enter keywords");
    inputPane.addRow(0, keywordsLabel); // Add instructions

    final TextField keywordsInput = new TextField();
    inputPane.addRow(1, keywordsInput);  // Add text field for key words

    // Set up button that will Initialize the twitter feed analysis
    Button startFeed = new Button("Start Twitter Feed");
    startFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(final ActionEvent arg0) {
          String[] words = new String[]{keywordsInput.getText()};
          model.setKeywords(words);
          model.start();
        }
    });
    inputPane.addRow(2, startFeed); // Add the start button

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
    * Model handles the interaction between Spark and the front end
    */
  public class Model extends Thread {
    private StringProperty stringProperty;

    /**
      * Variable to store the app's instance of the spark streamer.
      */
    private SparkStreamer sparkTweets;

    private String [] keywords;

    public Model() {
      stringProperty = new SimpleStringProperty(this, "int", "");
      setDaemon(true);
    }

    public String getString() {
      return stringProperty.get();
    }

    public StringProperty stringProperty() {
      return stringProperty;
    }

    public void setKeywords(String [] inputWords) {
      keywords = inputWords;
    }

    @Override
    public void start() {
      System.out.println((String)Array.get(keywords, 0));
      TwitterDataStream myStream = new TwitterDataStream(stringProperty);
      myStream.streamData(keywords);
      // try {
      //   sparkTweets = new SparkStreamer();
      // } catch (StreamingQueryException e) {
      //   System.out.println("Stream exception!");
      // }
    }
  }

  /**
    * Starts the program.
    */
  public static void main(final String[] args) {
      launch(args);
  }
}
