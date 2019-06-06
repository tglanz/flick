package tglanz.applications.jobs.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text;
        if (!params.has("input")){
            System.out.println("no input determined, try --input");
            return;
        }
        
        text = env.readTextFile(params.get("input"));

        DataSet<Tuple2<String, Integer>> counts = text
            .flatMap(new Tokenizer())
            .groupBy(0)
            .sum(1);
        
            if (params.has("output")){
                counts.writeAsCsv(params.get("output"), "\n", " ");
                env.execute("word count");
            } else {
                System.out.println("no `output` argument, will print results to stdout");
                counts.print();
            }
    }

    private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
