package com.tripartite.flinkdemo;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class StateQueryClient {
    private static final Logger LOG = LoggerFactory.getLogger(StateQueryClient.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        QueryableStateClient client = new QueryableStateClient("192.168.0.11", 9069);
        TupleTypeInfo<Tuple2<String, String>> tupleTypeInfo = new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(String.class));
        CompletableFuture<ValueState<Tuple2<String, String>>> kvState = client.getKvState(
                JobID.fromHexString("00000000000000000000000000000000"),
                "FK-join-state",
                "test-0:random-0",
                TypeInformation.of(String.class),
                new ValueStateDescriptor<>("FK-join-state", tupleTypeInfo));

        ValueState<Tuple2<String, String>> fkJoinValue = kvState.get();
        System.out.println("**************************************");
        System.out.println(fkJoinValue.value());
        System.out.println("**************************************");
    }
}
