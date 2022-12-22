// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;
import org.apache.hadoop.io.DoubleWritable;

public class RedditAverage extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private final static Long one = (long) 1;
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());
			// while (itr.hasMoreTokens()) {
			// 	word.set(itr.nextToken());
			// 	context.write(word, one);
            JSONObject record = new JSONObject(value.toString());
            System.out.println((String) record.get("subreddit"));
            System.out.println((Integer) record.get("score"));
            long score =  record.getLong("score");
            LongPairWritable pair = new LongPairWritable(one,score);
            String subreddit = record.get("subreddit").toString();
            word.set(subreddit);
			context.write(word, pair);

            // pair.set(2, 9);
            // System.out.println(pair.get_0()); // 2
            // System.out.println(pair.get_1()); // 9
			
		}
	}
	public static class AvgCombiner extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private DoubleWritable  avg = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
			Long occurance_sum = (long) 0;
			Long score_sum = (long) 0;

			for (LongPairWritable val : values) {
				occurance_sum += val.get_0();
                score_sum += val.get_1();
			}
            // double average;
            // average = score_sum/occurance_sum;
			// avg.set(average);
			LongPairWritable pair = new LongPairWritable();
			pair.set(occurance_sum, score_sum);
			context.write(key, pair);
		}
	}

	public static class AvgReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable  avg = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
			Double occurance_sum = (double) 0;
			Double score_sum = (double) 0;

			for (LongPairWritable val : values) {
				occurance_sum =occurance_sum + val.get_0();
                score_sum = score_sum + val.get_1();
			}
            Double average;
            average = (double) score_sum/occurance_sum;
			avg.set(average);
			context.write(key, avg);
		}
	}

	// public static class AvgReducer
	// extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
	// 	private DoubleWritable  avg = new DoubleWritable();

	// 	@Override
	// 	public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
	// 		Double occurance_sum = (double) 0;
	// 		Double score_sum = (double) 0;

	// 		for (LongPairWritable val : values) {
	// 			occurance_sum += val.get_0();
    //             score_sum += val.get_1();
	// 		}
    //         double average;
    //         average = score_sum/occurance_sum;
	// 		avg.set(average);
	// 		context.write(key, avg);
	// 	}
	// }

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Reddit Average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(AvgCombiner.class);
		job.setReducerClass(AvgReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}