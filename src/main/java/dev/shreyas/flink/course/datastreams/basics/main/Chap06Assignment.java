package dev.shreyas.flink.course.datastreams.basics.main;

import dev.shreyas.flink.course.utils.InputUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
    Data is of the following schema

    # cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count

    Using Data stream/Dataset transformations find the following for each ongoing trip.

    1.) Popular destination.  | Where more number of people reach.

    2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.

    3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made

 */
public class Chap06Assignment {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        InputUtil inputUtil = new InputUtil(parameterTool);

        String inputFile = inputUtil.getInput("input");
        DataStream<Tuple8<String,String,String,String,String,String,String,String>> dataStream =
                env.readTextFile(inputFile)
                .map(new RichMapFunction<String, Tuple8<String, String, String, String, String, String, String, String>>() {
                    @Override
                    public Tuple8<String, String, String, String, String, String, String, String> map(String s) throws Exception {
                        String[] pieces = s.split(",",8);
                        return new Tuple8<>(pieces[0],pieces[1],pieces[2],pieces[3],pieces[4],pieces[5],pieces[6],pieces[7]);
                    }
                });
        // 1.) Popular destination.
        dataStream.keyBy(6)
                .maxBy(6)
                .print();

        // Average number of passengers from each pickup location.
        dataStream
        .flatMap(new RichFlatMapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple3<String,Long, Long>>() {
            @Override
            public void flatMap(Tuple8<String, String, String, String, String, String, String, String> data, Collector<Tuple3<String,Long, Long>> collector) throws Exception {
                long passenger;
                if(data.f7==null || data.f7.equals("null") || data.f7.equals("")|| data.f7.equals("'null'")){
                    passenger=0;
                }else
                    passenger=Long.parseLong(data.f7);
                collector.collect(new Tuple3<>(data.f5,1L,passenger));
            }
        }).keyBy(0)
        .reduce(new RichReduceFunction<Tuple3<String,Long, Long>>() {
            @Override
            public Tuple3<String,Long, Long> reduce(Tuple3<String,Long, Long> t, Tuple3<String,Long, Long> t1) throws Exception {
                return new Tuple3<>(t.f0,t.f1+t1.f1,t.f2+t1.f2);
            }
        })
        .flatMap(new RichFlatMapFunction<Tuple3<String,Long, Long>, Tuple2<String,Double>>() {
            @Override
            public void flatMap(Tuple3<String, Long, Long> t, Collector<Tuple2<String, Double>> collector) throws Exception {
                collector.collect(new Tuple2<>(t.f0,(double)t.f2/t.f1));
            }
        })
        .print();

        dataStream
                .flatMap(new RichFlatMapFunction<Tuple8<String, String, String, String, String, String, String, String>, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(Tuple8<String, String, String, String, String, String, String, String> t, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(new Tuple2<>(t.f0,1));
                    }
                })
                .keyBy(0)
                .reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(t1.f0,t1.f1+stringIntegerTuple2.f1);
                    }
                })
        .print();
        env.execute("Blah");

    }
}
