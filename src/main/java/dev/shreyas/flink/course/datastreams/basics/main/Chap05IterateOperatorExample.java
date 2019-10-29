package dev.shreyas.flink.course.datastreams.basics.main;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Count how many items the numbers 0 to 5 have to be incremented
// in order to reach 10

public class Chap05IterateOperatorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Long,Long>> dataStream = env.generateSequence(0,5)
                .map(new RichMapFunction<Long, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Long aLong) throws Exception {
                        return new Tuple2<>(aLong,1L);
                    }
                });
        // prepare stream for iteration, will wait for 5000.
        IterativeStream<Tuple2<Long,Long>> iterativeStream = dataStream.iterate(5000);

        //define iteration
        DataStream<Tuple2<Long,Long>> plusOne=iterativeStream.map(new RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Tuple2<Long, Long> longLongTuple2) throws Exception {
                if(longLongTuple2.f0 == null)
                    return longLongTuple2;
                else
                    return new Tuple2<>(longLongTuple2.f0+1,longLongTuple2.f1+1);
            }
        });

        // part of stream to be used in next iteration
        DataStream<Tuple2<Long,Long>> notEqualToken = plusOne.filter(new FilterFunction<Tuple2<Long, Long>>() {
            @Override
            public boolean filter(Tuple2<Long, Long> longLongTuple2) throws Exception {
                return longLongTuple2.f0!=10;
            }
        });
        // feed data back to next iteration
        iterativeStream.closeWith(notEqualToken);

        // data not feedback to iteration
        DataStream<Tuple2<Long,Long>> equalToTen =plusOne.filter(new RichFilterFunction<Tuple2<Long, Long>>() {
            @Override
            public boolean filter(Tuple2<Long, Long> longLongTuple2) throws Exception {
                return longLongTuple2.f0==10;
            }
        });

        equalToTen.print();
        env.execute("Iterate numbers to 10 with count");
    }
}
