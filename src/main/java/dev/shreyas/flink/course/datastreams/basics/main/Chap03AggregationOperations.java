package dev.shreyas.flink.course.datastreams.basics.main;

import dev.shreyas.flink.course.utils.InputUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// flink run -c dev.shreyas.flink.course.datastreams.basics.main.Chap03AggregationOperations path/to/jar --input path/to/Sales.txt
public class Chap03AggregationOperations {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String inputFile = new InputUtil(parameterTool).getInput("input");

        // Read from Sales.txt
        DataStream<Tuple3<String, String, Integer>> dataStream = env.readTextFile(inputFile)
                .flatMap(new RichFlatMapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                        String[] pieces = s.split(",", 5);
                        collector.collect(new Tuple3<>(pieces[1], pieces[2], Integer.parseInt(pieces[4])));
                    }
                });

        /*
            Rolling aggregations on a keyed data stream. The difference between min and minBy is that min returns the minimum value,
            whereas minBy returns the element that has the minimum value in this field (same for max and maxBy).
         */
        dataStream.keyBy(0).min(2).print();
        dataStream.keyBy(0).minBy(2).print();
        dataStream.keyBy(0).max(2).print();
        dataStream.keyBy(0).maxBy(2).print();
        dataStream.keyBy(0).sum(2).print();
        env.execute("InBuilt KeyBy Aggregations");
    }

}
