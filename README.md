# WeblogChallenge

This is an interview challenge for Paytm Labs. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Running
In order to run the processing using `spark-submit` the project needs to be assembled into a fat jar first. Note that this project was implemented with Spark 1.6.0 and compiles into Scala 2.10.6 (can be changed in `build.sbt`).

To build the fat jar run:

```bash
sbt clean assembly
```

This will create a jar file containing all the required class files and dependencies (except spark which is declared as 'provided') in `target/scala-2.10/WeblogChallenge-assembly-1.0.jar`. This jar can then be used with `spark-submit`. The path to the input data file is specified by the `--inputPath <path to data.gz>` argument.

The run the processing:

```bash
spark-submit --master <MASTER-URL> \
--class net.antonvanco.paytm.weblogchallenge.Main \
/path/to/WeblogChallenge/target/scala-2.10/WeblogChallenge-assembly-1.0.jar \
--inputPath /path/to/data/2015_07_22_mktplace_shop_web_log_sample.log.gz
```

The run the unit tests:

```bash
sbt clean test
```

In order to run this task using `spark-submit` all usages of `SparkConf("appName", "local[*]")` need to be changed to `SparkConf("appName")` (since SparkConf.setMaster takes precedence over `spark-submit --master`)

##Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times



###Tools allowed (in no particular order):
- Spark (any language, but prefer Scala)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding
- Hive
If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


###Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



##How to complete this challenge:

A. Fork this repo in github
    https://github.com/PaytmLabs/WeblogChallenge

B. Complete the processing and analytics as defined first to the best of your ability with the time provided.

C. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the Paytm Labs interview team.

D. Complete your work in your own github repo and send the results to us and/or present them during your interview.

##What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.
