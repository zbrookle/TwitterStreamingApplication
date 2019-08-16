# TwitterStreamingApplication
A simple application with a JavaFX UI that allows users to view and filter live tweets. This application is built using JavaFX with spark and twitter4j backend.

# Dependencies
The dependencies for this project include spark, scala, and twitter4j. See the pom.xml for more details on dependencies and build.

# Notes
Please not that this uses spark as a backend which is meant to run or a cluster or some for of distributed computing. This application runs spark on two logical cores, which is not how spark is really meant to be used. For optimal performance it would be connected to some form of cluster and that would need to be changed in the master settings, found at
``` java
spark = builder.master("local[2]").appName("TwitterStream").getOrCreate();
```
