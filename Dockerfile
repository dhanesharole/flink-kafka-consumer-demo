FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY ./build/libs/flink-kafka-consumer-demo-0.1-SNAPSHOT.jar $FLINK_HOME/usrlib/flink-kafka-consumer-demo.jar
