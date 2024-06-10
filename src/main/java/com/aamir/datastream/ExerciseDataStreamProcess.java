package com.aamir.datastream;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExerciseDataStreamProcess {
    public static void main(String[] args) throws Exception {
        decideFizzBuzz();
    }

    private static void decideFizzBuzz() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> numbers = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            numbers.add(i);
        }

        DataStream<Integer> numbersList = env.fromCollection(numbers);

        DataStream<String> result = numbersList.process(new ProcessFunction<Integer, String>() {
            private List<Integer> fizzBuzzList;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                fizzBuzzList = new ArrayList<>();
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                if (value % 3 == 0 && value % 5 == 0) {
                    fizzBuzzList.add(value);
                    out.collect("fizzbuzz");
                } else if (value % 3 == 0) {
                    out.collect("fizz");
                } else if (value % 5 == 0) {
                    out.collect("buzz");
                } else {
                    out.collect(value.toString());
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("Collected fizzbuzz numbers:");
                fizzBuzzList.forEach(System.out::println);
            }
        });

        result.print();

        final StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(new Path("output/streaming"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                    DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024) // 1 GB
                        .build())
                .build();

        result.addSink(fileSink).name("StreamingFileSink");

        env.execute("Flink FizzBuzz Example");
    }
}