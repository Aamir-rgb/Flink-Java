package com.aamir.datastream;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExerciseDataStreamRichMap {

    private static void decideFizzBuzz() throws Exception {

        // Start the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Initialize a list
        List<Integer> numbers = new ArrayList<>();

        // Add numbers from 1 to 100 to the list
        for (int i = 1; i <= 100; i++) {
            numbers.add(i);
        }

        // Create a DataStream of the numbers from 1 to 100
        DataStream<Integer> numbersList = env.fromCollection(numbers);

        // Use a RichMapFunction to apply FizzBuzz logic and collect "fizzbuzz" numbers
        DataStream<String> result = numbersList.map(new RichMapFunction<Integer, String>() {
            private static final long serialVersionUID = 1L;
            private List<Integer> fizzBuzzList;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                fizzBuzzList = new ArrayList<>();
            }

            @Override
            public String map(Integer value) {
                if (value % 3 == 0 && value % 5 == 0) {
                    fizzBuzzList.add(value);
                    return "fizzbuzz";
                } else if (value % 3 == 0) {
                    return "fizz";
                } else if (value % 5 == 0) {
                    return "buzz";
                } else {
                    return value.toString();
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("Collected fizzbuzz numbers:");
                fizzBuzzList.forEach(System.out::println);
            }
        });

        // Print the result DataStream to the console
        result.print();

        // Execute the Flink job
        env.execute("Flink FizzBuzz Example");
    }

    public static void main(String[] args) throws Exception {
        decideFizzBuzz();
    }
}
