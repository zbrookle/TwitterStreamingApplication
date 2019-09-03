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
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
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
import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;

// CSV Output
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

// Spark
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.api.java.function.VoidFunction2;

// log4j
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

// RegExUtils
import org.apache.commons.lang3.RegExUtils;

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
    * Stores the data that goes inside the bar chart.
    */
  private ObservableList<XYChart.Data<String, Double>> barChartData;


  /**
    * Stores the x position of the window.
    */
  private double xPos;

  /**
    * Stores the y position of the window.
    */
  private double yPos;

  /**
    * The width of the screen.
    */
  private double screenWidth;

  /**
    * The height of the screen.
    */
  private double screenHeight;

  /**
    * The standard width accross each section of the interface.
    */
  private double columnWidth;

  /**
    * The thread that handles interactions with twitter.
    */
  private TwitterThread twitterThread;

  /**
    * The thread that handles interactions with spark.
    */
  private SparkThread sparkThread;

  /**
    * The gridpane where the twitter feed is displayed.
    */
  private GridPane twitterFeedPane;

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
      pieChartData.add(new javafx.scene.chart.PieChart.Data(name, value));
  }

  /**
    * Method for updating existing Data-Object if name matches.
    * @param name is the name of the element
    * @param value is the numeric value of the element
    */
  public void addDataBarChart(final String name, final double value) {
      for (XYChart.Data d : barChartData) {
          if (d.getXValue().equals(name)) {
              d.setYValue(value);
              return;
          }
      }
      barChartData.add(new XYChart.Data(name, value));
  }

  /**
    * Clears the stream files currently in the stream directory.
    **/
  private void clearStreamDir() {
    for (File file: new File("stream").listFiles()) {
       if (!file.isDirectory()) {
           file.delete();
       }
    }
  }

  /**
    * Generates the pane where all charts are displayed and updated.
    * @return chartPane the pane containing the charts
    **/
  private GridPane createChartPane() {
    GridPane chartPane = new GridPane();
    // Add pie chart
    pieChartData = FXCollections.observableArrayList();
    final PieChart piechart = new PieChart(pieChartData);
    piechart.setTitle("Language Distribution");
    chartPane.addRow(0, piechart);

    // Add bar chart
    final CategoryAxis xAxis = new CategoryAxis();
    final NumberAxis yAxis = new NumberAxis();
    final BarChart<String, Number> barchart = new BarChart<String, Number>(xAxis, yAxis);
    barchart.setTitle("Top 5 Tweet Word Frequencies");
    xAxis.setLabel("Word");
    yAxis.setLabel("Frequency");
    chartPane.addRow(1, barchart);
    barChartData = FXCollections.observableArrayList();
    XYChart.Series wordCounts = new XYChart.Series(barChartData);
    wordCounts.setName("Words");
    barchart.getData().add(wordCounts);

    // Set column size
    setRegionSize(chartPane, columnWidth * 4 / 3, screenHeight * .92);

    return chartPane;
  }

  /**
    * Generates the pane where the twitter feed is displayed.
    * @return twitterFeedPane the pane displaying the twitter feed
    **/
  private ScrollPane twitterFeedPane() {
    // Create an AtomicReference with a string to pass tweets between front and back end
    final AtomicReference<String> tweetText = new AtomicReference("");

    // Set content pane
    twitterFeedPane = new GridPane();
    twitterFeedPane.setPrefSize(columnWidth, screenHeight);
    twitterFeedPane.setMinSize(Control.USE_PREF_SIZE, Control.USE_PREF_SIZE);
    twitterFeedPane.setMaxWidth(Control.USE_PREF_SIZE);
    twitterFeedPane.setMaxHeight(Region.USE_COMPUTED_SIZE);

    // Set scrolling pane
    final ScrollPane feedScrollPane = new ScrollPane(twitterFeedPane);
    setRegionSize(feedScrollPane, columnWidth, screenHeight);
    feedScrollPane.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
    feedScrollPane.setVbarPolicy(ScrollPane.ScrollBarPolicy.AS_NEEDED);

    tweetCount = 0; // Variable to keep track of tweet row
    twitterThread = new TwitterThread();
    twitterThread.tweetProperty().addListener(new ChangeListener<String>() {
      @Override
      public void changed(final ObservableValue<? extends String> observable,
          final String oldValue, final String newValue) {
        if (tweetText.getAndSet(newValue.toString()) != "" && !twitterThread.getEndFeed().get()) {
          Platform.runLater(new Runnable() {
            @Override
            public void run() {
              double tweetBoxHeight = screenHeight / 12;
              boolean autoscroll = false;
              if (feedScrollPane.getVvalue() > .8) {
                autoscroll = true;
              }
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
              if (autoscroll) {
                feedScrollPane.setVvalue(feedScrollPane.getVmax());
              }
            }
          });
        }

      }
    });

    return feedScrollPane;
  }

  @Override
  public void start(final Stage primaryStage) throws Exception {
    // Create the stream directory if it doesn't exist
    File directory = new File("stream");
    if (!directory.exists()){
        directory.mkdir();
    }

    clearStreamDir(); // Clear the directory so that spark doesn't read old files

    final String instructionText = "Please enter keywords, separated by commas";

    // Create a root pane
    GridPane root = new GridPane();

    // Get screen bounds
    Rectangle2D screenBounds = Screen.getPrimary().getVisualBounds();
    xPos = screenBounds.getMinX();
    yPos = screenBounds.getMinY();
    screenWidth = screenBounds.getWidth();
    screenHeight = screenBounds.getHeight();
    columnWidth = screenWidth / 3;

    /* Chart column */
    final GridPane chartPane = createChartPane();

    /* Twitter feed */
    final ScrollPane feedScrollPane = twitterFeedPane();

    // Read in the ISO 639-1 codes for languages from language-codes.csv
    BufferedReader codeReader = new BufferedReader(new FileReader("language-codes.csv"));
    String codeLine = "";
    final HashMap<String, String> languageCodeMap = new HashMap<String, String>();

    while ((codeLine = codeReader.readLine()) != null) {
        String[] strings = codeLine.split(",");
        languageCodeMap.put(strings[0], strings[1]);
    }

    // Turn off the log messages for spark
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    /* Interface Column */
    double inputPaneVerticalSpacing = screenHeight / 100;
    VBox inputPane = new VBox(inputPaneVerticalSpacing);
    inputPane.setAlignment(Pos.BASELINE_RIGHT);

    final Label instructions = new Label(instructionText);

    HBox keywordsPane = new HBox();
    keywordsPane.setAlignment(Pos.BASELINE_RIGHT);
    Label keywordsLabel = new Label("Keywords:  ");
    final TextField keywordsInput = new TextField();
    keywordsPane.getChildren().addAll(keywordsLabel, keywordsInput);

    // Set up buttons that will initialize and end the twitter feed analysis
    final Button startFeed = new Button("Start Twitter Feed");
    final Button endFeed = new Button("End Twitter Feed");
    final AtomicBoolean chartsUpdatable = new AtomicBoolean(true);
    endFeed.setDisable(true);
    startFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(final ActionEvent arg0) {
          if (!(keywordsInput.getText() == null || keywordsInput.getText().trim().isEmpty())) {
            String[] words = keywordsInput.getText().split(",");

            // Clear bar and pie chart and corresponding data
            // atomicLanguageCounts.get().clear();
            // atomicWordCounts.get().clear();
            pieChartData.clear();
            barChartData.clear();

            // Clear tweets from feed
            twitterFeedPane.getChildren().clear();
            twitterFeedPane.setPrefHeight(screenHeight);

            clearStreamDir();

            tweetCount = 0;
            startFeed.setDisable(true);
            endFeed.setDisable(false);
            instructions.setText("Now streaming Twitter data");
            twitterThread.setKeywords(words);
            twitterThread.start();

            chartsUpdatable.set(true);

            // An atomic boolean for keeping track between the two interfaces of the
            // fact that the data has been refreshed.
            final AtomicBoolean dataRefresh = new AtomicBoolean(true);

            // An atomic wrapper to hold the map of language counts from the query.
            final AtomicReference<HashMap> atomicLanguageCounts = new AtomicReference(new HashMap<String, Double>());

            // An atomic wrapper to hold the map of word counts from the tweets.
            final AtomicReference<HashMap> atomicWordCounts = new AtomicReference(new HashMap<String, Double>());

            // Create a new thread that runs only spark processes
            try {
              sparkThread = new SparkThread(atomicLanguageCounts, dataRefresh, atomicWordCounts);
            } catch (StreamingQueryException e) {
              System.out.println("StreamingQueryException!");
            }
            sparkThread.dataRefreshProperty().addListener(new ChangeListener<Boolean>() {
              @Override
              public void changed(final ObservableValue<? extends Boolean> observable,
                  final Boolean oldValue, final Boolean newValue) {
                  if (chartsUpdatable.get()) {
                    // Disable chart update
                    chartsUpdatable.set(false);

                    // Schedule update for future
                    Platform.runLater(new Runnable() {
                      @Override
                      public void run() {
                        HashMap<String, Double> languageCounts = atomicLanguageCounts.get();
                        for (String key : languageCounts.keySet()) {
                            String language = languageCodeMap.get(key);
                            if (language == null) {
                              addDataPieChart(key, languageCounts.get(key));
                            } else {
                              addDataPieChart(language, languageCounts.get(key));
                            }
                        }

                        // Get and sort the words by count
                        HashMap<String, Double> wordCounts = atomicWordCounts.get();
                        List<HashMap.Entry<String, Double>> entries =
                          new ArrayList<HashMap.Entry<String, Double>>(wordCounts.entrySet());
                        Collections.sort(entries,
                                         new Comparator<HashMap.Entry<String, Double>>() {
                                           public int compare(final HashMap.Entry<String, Double> a,
                                                              final HashMap.Entry<String, Double> b) {
                                             return Double.compare(a.getValue(), b.getValue());
                                           }
                                         });
                        Collections.reverse(entries);
                        // Clear the data and add the current top 5 words
                        barChartData.clear();
                        if (entries.size() >= 5) {
                          for (int i = 0; i < 5; i++) {
                            addDataBarChart(entries.get(i).getKey(), entries.get(i).getValue());
                          }
                        }

                        // Reenable chart updating
                        chartsUpdatable.set(true);
                      }
                  });
                }
              }
            });
            sparkThread.start();
          }
        }
    });

    endFeed.setOnAction(new EventHandler<ActionEvent>() {
        @Override
        public void handle(final ActionEvent arg0) {
          startFeed.setDisable(false);
          endFeed.setDisable(true);
          twitterThread.getEndFeed().getAndSet(true);

          sparkThread.atomicWordCountsStream().get().stop();
          sparkThread.atomicLanguageCountsStream().get().stop();

          // Put the UI thread to sleep to allow for twitter to stop
          try {
            Thread.sleep(1500);
          } catch (InterruptedException e) {
            System.out.println("InterruptedException!");
          }

          // Clear feed
          instructions.setText(instructionText);
        }
    });
    setRegionSize(inputPane, columnWidth * 2 / 3, screenHeight);
    inputPane.getChildren().addAll(instructions, keywordsPane, startFeed, endFeed);

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

  @Override
  public void stop() {
      twitterThread.getEndFeed().set(true);
      twitterThread.stop();

      // Terminate spark streams
      sparkThread.atomicWordCountsStream().get().stop();
      sparkThread.atomicLanguageCountsStream().get().stop();
      sparkThread.stop();

      // Clear out files
      clearStreamDir();
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
      * An array of words that will be passed to twitter4j to get tweets.
      */
    private String[] keywords;

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
      * Constructor for the TwitterThread class.
      */
    public TwitterThread() {
      tweetProperty = new SimpleStringProperty(this, "string", "");
      endFeed = new AtomicBoolean(false);
      setDaemon(true);

      // Initialize the stream instance
      twitterStream = new TwitterStreamFactory().getInstance();
      partition = 0;

      // Set up the listener
      StatusListener listener = new StatusListener() {
        // Create place to store data
        public void onStatus(final Status status) {
            if (endFeed.get()) {
              twitterStream.cleanUp();
              partition = 0;
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
            tweetProperty.set(status.getUser().getName() + ":" + status.getText());

            // Create the csv file to write to
            try {
              writer = Files.newBufferedWriter(Paths.get("stream/data-" + Integer.toString(partition) + ".csv"));
              csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
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

    /**
      * Method that begins the streaming process.
      * @param inputKeywords is an array of key words to look for in tweets
      */
    public void streamData(final String[] inputKeywords) {
      // Create a new filter instance
      FilterQuery tweetFilterQuery = new FilterQuery();

      // Add the keywords to the query
      tweetFilterQuery.track(inputKeywords);

      // Start the stream
      twitterStream.filter(tweetFilterQuery);
    }

    @Override
    public void start() {
      endFeed.set(false);
      streamData(keywords);
    }
  }

  /**
    * SparkThread handles the interaction between Spark and the front end.
    */
  public class SparkThread extends Thread {
    /**
      * Variable to store the data refresh property to use for listening.
      */
    private BooleanProperty dataRefreshProperty;

    /**
      * Atomic language counts to pass to spark.
      */
    private AtomicReference<HashMap> atomicLanguageCounts;

    /**
      * Atomic language counts to pass to spark.
      */
    private AtomicReference<HashMap> atomicWordCounts;

    /**
      * Variable to communicate data refresh between spark and app.
      */
    private AtomicBoolean atomicDataRefresh;

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
      * Tracks the counts of words that appear in tweets.
      */
    private HashMap<String, Double> wordCountsMap;

    /**
      * Atomic wrapper for the language count StreamingQuery.
      **/
    private AtomicReference<StreamingQuery> atomicLanguageCountsStream;

    /**
      * Atomic wrapper for the word count StreamingQuery.
      **/
    private AtomicReference<StreamingQuery> atomicWordCountsStream;

    /**
      * Adds a word to the word counts hashmap if not there and otherwise
      * adds to the count of that word.
      * @param word is the string to be added
      */
    private void addToWordMap(final String word) {
      if (wordCountsMap.get(word) == null) {
        wordCountsMap.put(word, 1.0);
      } else {
        double count = wordCountsMap.get(word);
        count++;
        wordCountsMap.put(word, count);
      }
    }

    /**
      * Constructor for SparkThread.
      * @param languageCounts atomic reference to language counts
      * @param dataRefresh atomic reference to boolean controlling UI refresh
      * @param wordCounts atomic reference to word count map
      * @throws StreamingQueryException because it has a streaming query
      */
    public SparkThread(final AtomicReference<HashMap> languageCounts,
                       final AtomicBoolean dataRefresh,
                       final AtomicReference<HashMap> wordCounts) throws StreamingQueryException {
      dataRefreshProperty = new SimpleBooleanProperty(this, "bool", false);
      atomicLanguageCounts = languageCounts;
      atomicWordCounts = wordCounts;
      atomicDataRefresh = dataRefresh;
      setDaemon(true);

      // Initialize instance of the spark app
      Builder builder = new Builder();
      spark = builder.master("local[2]").appName("TwitterStream").getOrCreate(); // Use a small number of logical cores
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
    }

    @Override
    public void start() {
      final HashMap<String, Double> languageCountsMap = new HashMap<String, Double>();
      wordCountsMap = new HashMap<String, Double>();
      atomicLanguageCounts.set(languageCountsMap);

      String languageQuery = "select"
      + " case when language is null or language = 'und' then 'Undetermined'"
      + " else language end as language,"
      + " count(language) as tweet_count"
      + " from TWEET_DATA"
      + " group by language"
      + " having tweet_count > 0";
      // ^^^^^^^  this line shouldn't need to be here but
      // For some reason spark gives two different 'Undetermined' row counts
      // One of which is 0 so I'm filtering it out

      Dataset languageCountsDataSet = spark.sql(languageQuery);

      final DataStreamWriter languageCountsStreamWriter = languageCountsDataSet.writeStream()
        .outputMode("complete")
        .trigger(Trigger.ProcessingTime(200L))
        .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
          public void call(final Dataset<Row> dataset, final Long batchid) {
            List<Row> langCountsList = dataset.collectAsList();
            for (int i = 0; i < langCountsList.size(); i++) {
              String language = langCountsList.get(i).getString(0);
              Long languageCount = langCountsList.get(i).getLong(1);
              double languageCountDouble = languageCount.doubleValue();
              languageCountsMap.put(language, languageCountDouble); // Put the values in the map
              atomicLanguageCounts.set(languageCountsMap); // Set the atomic variable
            }
          }
        }
      );
      final StreamingQuery languageCountsStream = languageCountsStreamWriter.start();

      Dataset wordCountsDataset = spark.sql("select text, language from TWEET_DATA");
      final DataStreamWriter wordCountsStreamWriter = wordCountsDataset.writeStream()
        .outputMode("append")
        .trigger(Trigger.ProcessingTime(300L))
        .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
          public void call(final Dataset<Row> dataset, final Long batchid) {
            final List<String> allHashtags = new ArrayList<String>();
            Dataset<Row> tweetRows = dataset.select(dataset.col("Text"));
            List<Row> tweetRowList = tweetRows.collectAsList();
            for (int i = 0; i < tweetRowList.size(); i++) {
              String tweetText = tweetRowList.get(i).getString(0);
              // Get words from tweet
              if (tweetText != null) {
                String[] words = tweetText.split("\\s+");
                for (int j = 0; j < words.length; j++) {
                    // Replace punctuation and add to map
                    words[j] = RegExUtils.replaceAll(words[j], "[^\\w]", "");
                    //                    ^^^^ Apache commons replace is more efficient

                    // Only add on words that are likely not articles
                    if (words[j].length() > 3) {
                      addToWordMap(words[j].toLowerCase());
                    }
                }
                atomicWordCounts.set(wordCountsMap);
              }
              // Signal that an update has occurred
              if (dataRefreshProperty.get()) {
                dataRefreshProperty.set(false);
              } else {
                dataRefreshProperty.set(true);
              }
            }
          }
        });
        final StreamingQuery wordCountsStream = wordCountsStreamWriter.start();
        atomicWordCountsStream = new AtomicReference<StreamingQuery>();
        atomicLanguageCountsStream = new AtomicReference<StreamingQuery>();
        atomicWordCountsStream.set(wordCountsStream);
        atomicLanguageCountsStream.set(languageCountsStream);
    }

    /**
      * Get method for dataRefreshProperty.
      * @return dataRefreshProperty
      */
    public BooleanProperty dataRefreshProperty() {
      return dataRefreshProperty;
    }

    /**
      * Get method for atomicWordCountsStream.
      * @return atomicWordCountsStream
      **/
    public AtomicReference<StreamingQuery> atomicWordCountsStream() {
      return atomicWordCountsStream;
    }

    /**
      * Get method for atomicLanguageCountsStream.
      * @return atomicLanguageCountsStream
      **/
    public AtomicReference<StreamingQuery> atomicLanguageCountsStream() {
      return atomicLanguageCountsStream;
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
