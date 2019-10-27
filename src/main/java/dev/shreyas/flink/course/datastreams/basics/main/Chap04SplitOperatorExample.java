package dev.shreyas.flink.course.datastreams.basics.main;

import dev.shreyas.flink.course.utils.InputUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// Read a number and split it into two stream of odd or even
public class Chap04SplitOperatorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String inputFile = new InputUtil(parameterTool).getInput("input");

        String evenOutputFile = new InputUtil(parameterTool).getInput("evenOutput");
        String oddOutputFile = new InputUtil(parameterTool).getInput("oddOutput");
        DataStream<Integer> integerDataStream = env.readTextFile(inputFile)
                .flatMap(new RichFlatMapFunction<String, Integer>() {
                    @Override
                    public void flatMap(String s, Collector<Integer> collector) {
                        collector.collect(Integer.parseInt(s));
                    }
                });

        SplitStream<Integer> split = integerDataStream.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer integer) {
                List<String> output = new ArrayList<>();
                if (integer % 2 == 0) {
                    output.add("even");
                }
                else {
                    output.add("odd");
                }
                return output;
            }
        });
        DataStream<Integer> even = split.select("even");
        DataStream<Integer> odd = split.select("odd");
        System.out.println("Printing event numbers");
        even.writeAsText(evenOutputFile);
        System.out.println("Printing odd numbers");
        odd.writeAsText(oddOutputFile);
        env.execute("Split Operator example");
    }
}
