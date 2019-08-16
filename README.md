# TwitterStreamingApplication
A simple application with a JavaFX UI that allows users to view and filter live tweets. This application is built using JavaFX with spark and twitter4j backend.

# Dependencies
The dependencies for this project include spark, scala, and twitter4j. See the pom.xml for more details on dependencies and build.

# Notes
Please not that this uses spark as a backend which is meant to run or a cluster or some for of distributed computing. This application runs spark on two logical cores, which is not how spark is really meant to be used. For optimal performance it would be connected to some form of cluster and that would need to be changed in the master settings, found at
``` java
spark = builder.master("local[2]").appName("TwitterStream").getOrCreate();
```

# Use
To build the program use the given build command.
To run the program use the given run command with ./run. You may have to change this file depending on your platform.
Note that because this project uses spark, it was built with Java SDK 8. So make sure that your JAVA_HOME is pointing to a distribution of that version of java. ALSO since the project uses JavaFX this project will only work with an Oracle distribution, not with adoptopenjdk8.

# Twitter Connection
The application also requires a twitter4j.properties file to be placed in the directory with the following format. You can get these by registering an application on the twitter developer site.
```
debug=false
oauth.consumerKey=xxxxxxxxxxxxxxxx
oauth.consumerSecret=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
oauth.accessToken=xxxxxxxxxxxxxxxx
oauth.accessTokenSecret=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

# Interface


![If the app has loaded properly it looks like this.](https://github.com/zbrookle/TwitterStreamingApplication/tree/master/Pictures/BeforeRun.png)

![While running it looks like this.](https://github.com/zbrookle/TwitterStreamingApplication/tree/master/Pictures/DuringRun.png)
