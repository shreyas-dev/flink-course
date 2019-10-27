package dev.shreyas.flink.course.datastreams.basics.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// Basic Socket stream Word count Example

/* To Run

 Open Terminal 1: nc -l 9999
 Open Terminal 2: flink run -c dev.shreyas.flink.course.datastreams.basics.main.Chap01WordCountSocketExample path/to/jar/file


Test Output:
    (shreyas,1)
    (shreyas,2)
    (shreyas,3)
    (wassup,1)
    (people,1)
    (shreyaas,1)
 */

public class Chap01WordCountSocketExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Listens to socket port 9999 and reads the input
        DataStream<String> text = env.socketTextStream("localhost",9999);
        DataStream<Tuple2<String,Integer>> counts =
                text.flatMap(new Tokenizer())
                .keyBy(0) // Equivalent to Group By field
                .sum(1); // initial sum
        counts.print();
        env.execute("Word Count example");
    }

    // User-defined functions
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
