random-events
=============

Generate random events

## Build
mvn install

## Copy
scp target/random-events-\*.jar hduser@edge.example.com:
scp src/main/avro/standard\_event.avsc hduser@edge.example.com:

## Run
ssh hduser@example.com
hdfs dfs -put standard\_event.avsc
kite-dataset create events -s standard\_event.avsc -f parquet
java -jar random-events-\*.jar flume.example.com 41415 hdfs://nn.example.com/user/hduser/standard\_event.avsc
