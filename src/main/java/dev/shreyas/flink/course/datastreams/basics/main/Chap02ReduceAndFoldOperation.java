package dev.shreyas.flink.course.datastreams.basics.main;

import dev.shreyas.flink.course.utils.InputUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// Finding Avg units sold per month for each category

// The only difference between fold and reduce operator is reduce return the same output as input
// and fold output / return type can vary.
public class ReduceAndFoldOperation {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String inputFile = new InputUtil(parameterTool).getInput("input");

        // Read from Sales.txt
        DataStream<Tuple5<String,String,String,Integer,Double>> dataStream = env.readTextFile(inputFile)
                // convert csv to Tuple
                .flatMap(new RichFlatMapFunction<String, Tuple5<String, String, String, Integer, Double>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple5<String, String, String, Integer, Double>> collector) throws Exception {
                        String[] pieces = s.split(",", 5);
                                                      // June ,  category,  Bat, count , quantity
                        collector.collect(new Tuple5<>(pieces[1],pieces[2],pieces[3],1,Double.parseDouble(pieces[4])));
                    }
                });
        dataStream.keyBy(1) // group by month
                .keyBy(2) // group by category
                // Takes two tuples and reduces it to one and does it recursively using accumulator
                .reduce(new RichReduceFunction<Tuple5<String, String, String, Integer, Double>>() {
                    @Override
                    public Tuple5<String, String, String, Integer, Double> reduce(Tuple5<String, String, String, Integer, Double> t1, Tuple5<String, String, String, Integer, Double> t2) throws Exception {
                        // f3 is accumulated counter -> counts number of records
                        return new Tuple5<>(t1.f0,t1.f1,t1.f2,t1.f3+t2.f3,t1.f4+t2.f4);
                    }
                })
                .flatMap(new RichFlatMapFunction<Tuple5<String, String, String, Integer, Double>, Tuple3<String,String,Double>>() {
                    @Override
                    public void flatMap(Tuple5<String, String, String, Integer, Double> t, Collector<Tuple3<String,String, Double>> collector) throws Exception {
                        collector.collect(new Tuple3<>(t.f0,t.f1,t.f4/t.f3));
                    }
                }).print();
        env.execute("Calculate Avg units sold / category / month");


        /*
            Sample Output
                (July,Category4,34.333333333333336)
                (August,Category3,31.7)
                (July,Category2,35.38461538461539)
                (August,Category5,28.0)
                (June,Category3,32.0)
                (June,Category4,30.285714285714285)
                (June,Category4,23.428571428571427)
                (June,Category1,24.307692307692307)
                (August,Category3,29.2)
                (August,Category4,33.1875)
                (August,Category3,32.72727272727273)
                (June,Category4,30.636363636363637)
                (August,Category5,27.615384615384617)
                (August,Category3,28.09090909090909)
                (July,Category2,35.214285714285715)
                (August,Category4,33.588235294117645)
                (June,Category5,27.875)
                (August,Category3,27.916666666666668)
                (August,Category3,33.916666666666664)
                (July,Category1,31.5)
                (August,Category4,33.77777777777778)
                (August,Category3,29.076923076923077)
                (July,Category1,38.0)
                (July,Category1,38.69230769230769)
                (June,Category1,26.142857142857142)
                (July,Category4,35.30769230769231)
         */
    }
}
