package dev.shreyas.flink.course.batching.joins.main;

import dev.shreyas.flink.course.utils.InputUtil;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;


// to run ->
// flink run path/to/file/flink-course-1.0-SNAPSHOT.jar -c dev.shreyas.flink.course.batching.joins.main.BatchInnerJoinExample --location path/to/location/file --person path/to/location/person


// Inner Join
// An INNER JOIN is such type of join that returns all rows from both the participating tables where the
// key record of one table is equal to the key records of another table.
public class BatchInnerJoinExample {

    // Flat Map Function which splits the string by comma and creates tuple of 2
    public static class ReadInput extends RichFlatMapFunction<String, Tuple2<Integer, String>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<Integer, String>> collector) throws Exception {
                String[] pieces = s.split(",", 2);
                collector.collect(new Tuple2<>(Integer.parseInt(pieces[0]),pieces[1]));
        }
    }
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Input Utility
        InputUtil inputUtil = new InputUtil(ParameterTool.fromArgs(args));

        DataSet<Tuple2<Integer,String>> locationDataSet = env.readTextFile(inputUtil.getInput("location"))
                .flatMap(new ReadInput());

        DataSet<Tuple2<Integer,String>> personDataSet = env.readTextFile(inputUtil.getInput("person"))
                .flatMap(new ReadInput());

        DataSet<Tuple3<Integer,String,String>> joined = personDataSet.join(locationDataSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>,Tuple2<Integer,String>,Tuple3<Integer,String,String>>(){
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> personDataSet, Tuple2<Integer, String> locationDataSet) throws Exception {
                        return new Tuple3<>(personDataSet.f0,personDataSet.f1,locationDataSet.f1);
                    }
                });
        joined.print();
        // Prints
        //  (1,John,DC)
        //  (2,Albert,NY)
        //  (4,Smith,LA)
    }
}
