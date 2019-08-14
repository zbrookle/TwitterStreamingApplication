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
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

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
import java.util.List;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.functions;

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
            twitterStream.cleanUp();
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
  SparkStreamer(final AtomicReference<HashMap> atomicLanguageCounts, final BooleanProperty dataRefresh) throws StreamingQueryException {
    // Initialize instance of the spark app
    Builder builder = new Builder();
    spark = builder.master("local").appName("TwitterStream").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    spark.sparkContext().getConf().set("spark.streaming.stopGracefullyOnShutdown", "true");

    // Create Schema for data
    schema = new StructType(new StructField[] {
      new StructField("UserName", DataTypes.StringType, true, Metadata.empty()),
      new StructField("UserID", DataTypes.LongType, true, Metadata.empty()),
      new StructField("created_at", DataTypes.StringType, true, Metadata.empty()),
      new StructField("TextRangeStart", DataTypes.IntegerType, true, Metadata.empty()),
      new StructField("TextRangeEnd", DataTypes.IntegerType, true, Metadata.empty()),
      new StructField("FavoriteCount", DataTypes.IntegerType, true, Metadata.empty()),
      new StructField("Language", DataTypes.StringType, true, Metadata.empty()),
      new StructField("Place", DataTypes.StringType, true, Metadata.empty()),
      new StructField("Coordinates", DataTypes.StringType, true, Metadata.empty()),
      new StructField("RetweetCount", DataTypes.IntegerType, true, Metadata.empty()),
      new StructField("Text", DataTypes.StringType, true, Metadata.empty()),
      new StructField("isRetweet", DataTypes.BooleanType, true, Metadata.empty())
    });

    // Define where to read the twitter data from
    csvDF = spark.readStream().option("sep", ",").schema(schema).csv("stream/data*.csv");
    try {
      csvDF.createTempView("TWEET_DATA");
    } catch (AnalysisException e) {
      System.out.println("ERROR: Problem creating temp view");
    }

    final HashMap<String, Double> languageCountsMap = new HashMap<String, Double>();
    atomicLanguageCounts.set(languageCountsMap);
    String sqlQuery = "select"
    + " Username,"
    + " FavoriteCount,"
    + " TextRangeEnd,"
    + " case when Language = 'en' then 'English'"
    + " when Language = 'und' then 'Undetermined'"
    + " when Language = 'ht' then 'Haitian'"
    + " when Language = 'ja' then 'Japanese'"
    + " when Language = 'ru' then 'Russian'"
    + " when Language = 'in' then 'Indonesian'"
    + " when Language = 'fr' then 'French'"
    + " when Language = 'es' then 'Spanish'"
    + " when Language = 'pt' then 'Portugese'"
    + " when Language = 'th' then 'Thai'"
    + " when Language = 'tl' then 'Tagalog'"
    + " else 'Unknown' end as Language, "
    + " 1 as tweet_count, "
    + " Text"
    + " from TWEET_DATA";
    Dataset twitterDataSet = spark.sql(sqlQuery);
    final List<String> allHashtags = new ArrayList<String>();
    // Create and compile regex to match hashtag
    final Pattern hashtagPattern = Pattern.compile("#.*\\s");
    query = twitterDataSet.writeStream()
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(200L))
      .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
        public void call(final Dataset<Row> dataset, final Long batchid) {
          Dataset<Row> tweetRows = dataset.select(dataset.col("Text"));
          List<Row> tweetRowList = tweetRows.collectAsList();
          for (int i = 0; i < tweetRowList.size(); i++) {
            String tweetText = tweetRowList.get(i).getString(0);
            // Get hashtag data
            if (tweetText != null) {
              //Matcher matcher = hashtagPattern.matcher(tweetText);
              String[] matches = org.mentaregex.match(tweetText, "#[a-zA-Z0-9]+");
              System.out.println(matches);
              }
            }
          }

          // Get language Count data
          Dataset<Row> langCounts = dataset.groupBy(dataset.col("Language"))
                                           .agg(functions.sum(dataset.col("tweet_count")));
          List<Row> langCountsList = langCounts.collectAsList();
          for (int i = 0; i < langCountsList.size(); i++) {
            String language = langCountsList.get(i).getString(0);
            Long languageCount = langCountsList.get(i).getLong(1);
            double languageCountDouble = languageCount.doubleValue();
            if (languageCountsMap.containsKey(language)) {
              double currentLanguageCount = languageCountsMap.get(language);
              languageCountsMap.put(language, currentLanguageCount + languageCountDouble);
            } else {
              languageCountsMap.put(language, languageCountDouble);
            }
            atomicLanguageCounts.set(languageCountsMap);
          }
          if (dataRefresh.get()) {
            dataRefresh.set(false);
          } else {
            dataRefresh.set(true);
          }
        }
      }).start();
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

  /**
    * Stores the data that goes inside the pie chart.
    */
  private ObservableList<PieChart.Data> pieChartData;

  /**
    * Sets the size of various UI elements.
    * @param region is the UI element
    * @param width is the width
    * @param height is the height
    */
  private void setRegionSize(final Region region, final double width, final double height) {
    region.setPrefSize(width, height);
    region.setMinSize(Control.USE_PREF_SIZE, Control.USE_PREF_SIZE);
    region.setMaxSize(Control.USE_PREF_SIZE, Control.USE_PREF_SIZE);
  }

  /**
    * Method for adding a new data element to the pie chart.
    * @param name is the name of the element
    * @param value is the numeric value of the element
    */
  public void naiveAddDataPieChart(final String name, final double value) {
      pieChartData.add(new javafx.scene.chart.PieChart.Data(name, value));
  }

  /**
    * Method for updating existing Data-Object if name matches.
    * @param name is the name of the element
    * @param value is the numeric value of the element
    */
  public void addDataPieChart(final String name, final double value) {
      for (javafx.scene.chart.PieChart.Data d : pieChartData) {
          if (d.getName().equals(name)) {
              d.setPieValue(value);
              return;
          }
      }
      naiveAddDataPieChart(name, value);
  }

  @Override
  public void start(final Stage primaryStage) throws Exception {
    // Create a root pane
    GridPane root = new GridPane();

    // Create an AtomicReference with a string to pass tweets between front and back end
    final AtomicReference<String> tweetText = new AtomicReference("");

    // Get screen bounds
    Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
    final double xPos = screenBounds.getMinX();
    final double yPos = screenBounds.getMinY();
    final double screenWidth = screenBounds.getWidth();
    final double screenHeight = screenBounds.getHeight() - screenBounds.getHeight() / 100;
    final double columnWidth = screenWidth / 3;

    /* Chart column */
    GridPane chartPane = new GridPane();
    pieChartData = FXCollections.observableArrayList();
    final PieChart piechart = new PieChart(pieChartData);
    piechart.setTitle("Language Distribution");
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
    final TwitterThread twitterThread = new TwitterThread();
    twitterThread.tweetProperty().addListener(new ChangeListener<String>() {
      @Override
      public void changed(final ObservableValue<? extends String> observable,
          final String oldValue, final String newValue) {
        if (tweetText.getAndSet(newValue.toString()) != "" && !twitterThread.getEndFeed().get()) {
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

    // An atomic boolean for keeping track between the two interfaces of the
    // fact that the data has been refreshed.
    final AtomicBoolean dataRefresh = new AtomicBoolean(true);

    // An atomic wrapper to hold the map of language counts from the query.
    final AtomicReference<HashMap> atomicLanguageCounts = new AtomicReference(new HashMap<String, Double>());

    final SparkThread sparkThread = new SparkThread(atomicLanguageCounts, dataRefresh);
    sparkThread.dataRefreshProperty().addListener(new ChangeListener<Boolean>() {
      @Override
      public void changed(final ObservableValue<? extends Boolean> observable,
          final Boolean oldValue, final Boolean newValue) {
          Platform.runLater(new Runnable() {
            @Override
            public void run() {
              HashMap<String, Double> languageCounts = atomicLanguageCounts.get();
              for (String key : languageCounts.keySet()) {
                  addDataPieChart(key, languageCounts.get(key));
              }
            }
        });
      }
    });
    sparkThread.start();

    /* Interface Column */
    double inputPaneVerticalSpacing = screenHeight / 100;
    VBox inputPane = new VBox(inputPaneVerticalSpacing);
    inputPane.setAlignment(Pos.BASELINE_RIGHT);

    final Label instructions = new Label("Please enter keywords");

    HBox keywordsPane = new HBox();
    keywordsPane.setAlignment(Pos.BASELINE_RIGHT);
    Label keywordsLabel = new Label("Keywords:  ");
    final TextField keywordsInput = new TextField("football");
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
          twitterThread.setKeywords(words);
          twitterThread.start();
        }
    });

    endFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(final ActionEvent arg0) {
          startFeed.setDisable(false);
          endFeed.setDisable(true);
          twitterThread.getEndFeed().getAndSet(true);

          // Put the UI thread to sleep to allow for twitter to stop
          try {
            Thread.sleep(1500);
          } catch (InterruptedException e) {
            System.out.println("InterruptedException!");
          }
          // After twitter has stopped, delete the stream files
          for (File file: new File("stream").listFiles()) {
             if (!file.isDirectory()) {
                 file.delete();
             }
          }
          // Clear feed
          twitterFeedPane.getChildren().clear();
          tweetCount = 0;
          instructions.setText("Please enter keywords");

          // Clear pie chart and corresponding data
          atomicLanguageCounts.get().clear();
          pieChartData.clear();
        }
    });
    setRegionSize(inputPane, columnWidth, screenHeight);
    inputPane.getChildren().addAll(instructions, keywordsPane, startFeed, endFeed);

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
    * TwitterThread handles the interaction between Twitter and the front end.
    */
  public class TwitterThread extends Thread {
    /**
      * Wrapper for the text of the tweet.
      */
    private StringProperty tweetProperty;

    /**
      * Atomic boolean that determines whether to end the threads.
      */
    private AtomicBoolean endFeed;

    /**
      * Variable used to store the app's instance of the twitter data stream.
      */
    private TwitterDataStream myStream;

    /**
      * An array of words that will be passed to twitter4j to get tweets.
      */
    private String[] keywords;

    /**
      * Constructor for twitterThread.
      */
    public TwitterThread() {
      tweetProperty = new SimpleStringProperty(this, "string", "");
      endFeed = new AtomicBoolean(false);
      setDaemon(true);
    }

    /**
      * Get method for the endFeed AtomicBoolean.
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
      myStream = new TwitterDataStream(tweetProperty, endFeed);
      myStream.streamData(keywords);
    }
  }

  /**
    * SparkThread handles the interaction between Spark and the front end.
    */
  public class SparkThread extends Thread {
    /**
      * Variable to store the app's instance of the spark streamer.
      */
    private SparkStreamer sparkTweets;

    /**
      * Variable to store the data refresh property to use for listening
      */
    private BooleanProperty dataRefreshProperty;

    /**
      * Atomic language counts to pass to spark
      */
    private AtomicReference<HashMap> atomicLanguageCounts;

    /**
      * Variable to communicate data refresh between spark and app
      */
    private AtomicBoolean atomicDataRefresh;

    /**
      * Constructor for SparkThread.
      */
    public SparkThread(AtomicReference<HashMap> languageCounts, AtomicBoolean dataRefresh) {
      dataRefreshProperty = new SimpleBooleanProperty(this, "bool", false);
      atomicLanguageCounts = languageCounts;
      atomicDataRefresh = dataRefresh;
      setDaemon(true);
    }

    @Override
    public void start() {
      try {
        sparkTweets = new SparkStreamer(atomicLanguageCounts, dataRefreshProperty);
      } catch (StreamingQueryException e) {
        System.out.println("Stream exception!");
      }
    }

    /**
      * Get method for dataRefreshProperty.
      * @return dataRefreshProperty
      */
    public BooleanProperty dataRefreshProperty() {
      return dataRefreshProperty;
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
