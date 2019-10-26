package dev.shreyas.flink.course.batching.joins.main;

import dev.shreyas.flink.course.utils.InputUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

// LEFT JOIN performs a join starting with the first (left-most) table and then any matching second (right-most) table records.

// RIGHT JOIN performs a join starting with the second (right-most) table and then any matching first (left-most) table records.

// FULL JOIN returns all matching records from both tables whether the other table matches or not.


// to run ->
// flink run path/to/file/flink-course-1.0-SNAPSHOT.jar -c dev.shreyas.flink.course.batching.joins.main.BatchOtherJoinsExample --location path/to/location/file --person path/to/location/person


public class BatchOtherJoinsExample {

    public static class Join extends RichJoinFunction<Tuple2<Integer,String>,Tuple2<Integer,String>,Tuple3<Integer,String,String>> {
        @Override
        public Tuple3<Integer, String, String> join(Tuple2<Integer, String> personDataSet, Tuple2<Integer, String> locationDataSet) throws Exception {
            // Since it's left outer join, location can be Null
            // In case of right outer join, person can be null
            if (locationDataSet == null){
                return new Tuple3<>(personDataSet.f0,personDataSet.f1,"NULL");
            }
            return new Tuple3<>(personDataSet.f0,personDataSet.f1,locationDataSet.f1);
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Input Utility
        InputUtil inputUtil = new InputUtil(ParameterTool.fromArgs(args));

        DataSet<Tuple2<Integer,String>> locationDataSet = env.readTextFile(inputUtil.getInput("location"))
                .flatMap(new BatchInnerJoinExample.ReadInput());

        DataSet<Tuple2<Integer,String>> personDataSet = env.readTextFile(inputUtil.getInput("person"))
                .flatMap(new BatchInnerJoinExample.ReadInput());

        // perform left outer join
        DataSet<Tuple3<Integer,String,String>> leftOuterJoin = personDataSet.leftOuterJoin(locationDataSet).where(0).equalTo(0)
                .with(new Join());
        leftOuterJoin.print();
        /*
        Prints
            (1,John,DC)
            (2,Albert,NY)
            (3,Lui,NULL)
            (4,Smith,LA)
            (5,Robert,NULL)
         */
    }
}
