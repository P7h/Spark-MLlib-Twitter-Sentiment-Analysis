# Visualization of Twitter Sentiment using Spark MLlib 


## Introduction
Project to analyse and visualize Sentiment of tweets in real-time on a world map using Spark MLlib.

Docker image [hosted on Docker Hub](https://hub.docker.com/r/p7hb/p7hb-docker-mllib-twitter-sentiment) is available with the complete environment and dependencies installed. For detailed info on this project, please check the [blogpost](http://P7h.org/blog/2016/08/21/spark-twitter-sentiment/).<br>
Dockerfile and other supporting files are also available on [GitHub](https://github.com/P7h/p7hb-docker-mllib-twitter-sentiment).


## Twitter Sentiment Visualization Demo
### Demo of Visualization
![Demo of Twitter Sentiment Visualization](Twitter_Sentiment_Visualization.gif)

### Screenshot of Visualization
![Screenshot of Twitter Sentiment Visualization](Twitter_Sentiment_Visualization.png)


## Features
* Application retrieves tweets with Twitter Streaming API (using [Twitter4J](http://twitter4j.org)).
* Process and analyses sentiment of all the tweets [which have location] using Spark MLlib and also Stanford CoreNLP.
* Application can also save compressed raw tweets to the disk.
	* Please set `SAVE_RAW_TWEETS` flag to `true` in [`application.conf`](src/main/resources/application.conf#L30) if you want to save / retain the raw tweets we retrieve from Twitter.
* [Datamaps](https://datamaps.github.io/) -- based on [D3.js](https://d3js.org/) -- is used for visualization to display the tweet location on the world map with a pop up for more details on hover.
	* Visualization is fully responsive and automatically adjusts based on the resolution / resizing of the window. Works even on mobile.
	* Hover over the bubbles to see the additional info of the tweets.
* This codebase has been updated with comments, where necessary.
* TBD


## Dependencies

Following is the complete list of languages and frameworks used and their significance in this project.

1. OpenJDK 64-Bit v1.8.0_102 » Java for compiling and execution
2. Scala v2.10.6 » basic infrastructure and Spark jobs
3. SBT v0.13.12 » build file for scala code
4. Apache Spark v1.6.2
	* Spark Streaming » connecting to Twitter and streaming the tweets
	* Spark MLlib » creating a ML model and predicting the sentiment of tweets based on the text
	* Spark SQL » saving tweets [both raw and classified]
5. Stanford CoreNLP v3.6.0 » alternative mechanism to find sentiment of tweets based on the text
6. Redis » publishing classified tweets; subscribed by the front-end app to render the chart
7. Datamaps » charting and visualization
8. Python » running the flask app for rendering the front-end
9. Flask » rendering the template for front-end

Also, please check [`build.sbt`](build.sbt) for more information on the various other dependencies of the project.<br>


## Prerequisites for successful execution

* A machine with Docker installed and in which you can allocate at least the following [actually the more, the merrier] to the Docker-machine instance:
	* 2 GB RAM
	* 2 CPUs
	* 6 GB free disk space
* We will need unfettered internet access for executing this project.
* Twitter App OAuth credentials are mandatory. 
	* This is for retrieving tweets in real-time using Twitter Streaming API.
* We will download ~1.5 GB of data with this image and SBT dependencies, etc and streaming tweets too.

### Env Setup
If not already installed, please install Docker on your machine referring [installation steps](https://docs.docker.com/engine/installation/) on Docker website.

We will be using the accompanying [Docker image](https://hub.docker.com/r/p7hb/p7hb-docker-mllib-twitter-sentiment) created for this project.

### Resources for the Docker machine
* Stop docker-machine.

	`docker-machine stop default`

* Launch VirtualBox and click on settings of `default` instance, which should be in `Powered Off` state.
* Fix the settings as highlighted in the screenshots below. Please note this is minimum required config; you might want to allocate more.
* Increase RAM of the VM
	![Docker Machine RAM](Docker_Machine__RAM.png)
* Increase # of CPUs of the VM
	![Docker Machine CPU](Docker_Machine__CPU.png)
* Relaunch docker after modifying the settings.
* Any Docker image you create now will have 2 GB RAM and 2 CPUs allocated.
	* Or the resources you allocated earlier.


## Execution

### Run the Docker image
* This step will pull the Docker image from Docker Hub and runs it. After image boots up and completes the setup process, you will land into a bash shell waiting for your input.

    docker run -ti -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 9999:9999 -h spark --name=spark p7hb/p7hb-docker-mllib-twitter-sentiment:1.6.2

Please note:

 * `root` is the user we logged into.
 * `spark` is the container name.
 * `spark` is host name of this container. 
 	* This is very important as Spark Slaves are started using this host name as the master.
 * Exposes ports 4040, 8080, 8081 for Spark Web UI console and 9999 for Twitter Sentiment Visualization.


### Twitter App OAuth credentials
* The only manual intervention required in this project is setting up a Twitter App and updating its OAuth credentials to connect to Twitter Streaming API. Please note that this is a critical step and without this, Spark will not be able to connect to Twitter or retrieve tweets with Twitter Streaming API and so, the visualization will be empty basically without any data.
* Please check the [`application.conf`](src/main/resources/application.conf#L8-11) and add your own values and complete the integration of Twitter API to your application by looking at your values from [Twitter Developer Page](https://dev.twitter.com/apps).
	* If you did not create a Twitter App before, then please create a new Twitter App on [Twitter Developer Page](https://dev.twitter.com/apps), where you will get all the required values of `application.conf`.


### Run Spark jobs
* Please execute [`/root/exec_spark_jobs.sh`](https://github.com/P7h/p7hb-docker-mllib-twitter-sentiment/blob/master/exec_spark_jobs.sh) in the console after updating the Twitter App OAuth credentials in `application.conf`.
	* This script first starts Spark services [Spark Master and Spark Slave] and then launches Spark jobs one after the other.
* This might take sometime as SBT will initiate a download and setup of all the required packages from Maven Central Repo and Typesafe repo as required.


### Visualization app
After a few minutes of launching the Spark jobs, point your browser on the host machine to [`http://192.168.99.100:9999/`](http://192.168.99.100:9999/) to view the Twitter Sentiment visualized on a world map.<br>
Hover over a bubble to see additional info about that data point.


## TODO
* TBD
* Visualization could be completely scrapped for something better and UX needs a lot of uplifting.
* Use Spark wrapper for [Stanford CoreNLP](https://spark-packages.org/package/databricks/spark-corenlp) and reduce the boilerplate code further.
* Add or update comments in the code where necessary.
* Update the project to Spark v2.0.
	* Push out RDDs and use `org.apache.spark.ml` package; hello DataFrames and Datasets!


## Expert Mode execution steps
This is a very quick summary of the steps required for execution of this code.<br>
Please consider these steps only if you are an expert on Docker, Spark and ecosystem of this project and understand clearly what is being done here.

* Install and launch Docker.
* Stop Docker and in the VirtualBox GUI, increase RAM of Docker machine [instance named `default` and should be in `Powered Off` state] to at least 2 GB [or more] and # of CPUs to 2 [or more].
* Start Docker again.
* Pull the project Docker image and launch it.
	* Might have to wait for ~10 minutes or so [depending on your internet speed].

	`docker run -ti -p 4040:4040 -p 8080:8080 -p 8081:8081 -p 9999:9999 -h spark --name=spark p7hb/p7hb-docker-mllib-twitter-sentiment:1.6.2`

* Update [`application.conf`](src/main/resources/application.conf#L8-11) to include your Twitter App OAuth credentials.
* Execute: `/root/exec_spark_jobs.sh`
	* Might have to wait for ~10 minutes or so [depending on your internet speed].
* Point your browser on the host machine to [`http://192.168.99.100:9999`](http://192.168.99.100:9999) for visualization.


> ###NOTE:
Please do not forget to modify the Twitter App OAuth credentials in the file [`application.conf`](src/main/resources/application.conf#L8-11).<br>
Please check [Twitter Developer page](https://dev.twitter.com/apps) for more info. 


## License
Copyright &copy; 2016 Prashanth Babu.<br>
Licensed under the [Apache License, Version 2.0](LICENSE).
