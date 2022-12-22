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

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		// private final static IntWritable one = new IntWritable(1);
		//LongWritable new_requests = new LongWritable();
		private Text date = new Text();
        
        private LongWritable requests = new LongWritable();
        

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String lang = "";
			String title = "";
			String bytesize = "";

			while (itr.hasMoreTokens()) {                
				date.set(itr.nextToken());
                lang = itr.nextToken();
                title = itr.nextToken();
                requests.set(Long.parseLong(itr.nextToken()));
                bytesize = itr.nextToken();
			}
			
			if (lang.equals("en") && !(title.equals("Main_Page") || title.startsWith("Special:")))
            	context.write(date, requests);
                
        }
                
	}

	public static class MaxReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable maxVal = new LongWritable();
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long max = 0;
			for (LongWritable req : values) {
                if (req.get() > max){
                    max = req.get();
                }
			}
			maxVal.set(max);
			context.write(key, maxVal);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wiki popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}