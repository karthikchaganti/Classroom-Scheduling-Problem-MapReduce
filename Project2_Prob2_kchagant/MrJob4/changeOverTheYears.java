package MapReduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class changeOverTheYears {

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private Text valMap = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(","); 
			if (fields[1].equals("Unknown")
					|| fields[2].equals("Unknown")
					|| fields[2].equals("Arr Arr")
					|| fields[3].equals("UNKWN")
					|| fields[3].equals("ARR")
					|| fields[4].equals("Unknown")
					|| fields[4].equals("Before 8:00AM")
					|| fields.length != 9
					|| Integer.parseInt(fields[7]) <= 0
					|| Integer.parseInt(fields[8]) <= 0
					|| Integer.parseInt(fields[7]) > Integer
							.parseInt(fields[8])
					|| !StringUtils.isNumeric(fields[7])) {
				return;
			}
			word.set(fields[1] + "," + fields[2]);
			valMap.set(fields[7] + "," + fields[8]);
			context.write(word, valMap);
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sumEnr = 0;
			int sumCap = 0;
			for (Text val : values) {
				sumEnr += Integer.parseInt(val.toString().split(",")[0]);
				sumCap += Integer.parseInt(val.toString().split(",")[1]);
			}
			result.set(String.valueOf(sumEnr) + "," + String.valueOf(sumCap));
			context.write(key, result);
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text valMap = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] fields = value.toString().split("\\t");
			String key1 = fields[0].split(",")[0]
					+ "-"
					+ Integer
							.toString((Integer.parseInt(fields[0].split(",")[0]
									.split(" ")[1]) + 1)) + "_"
					+ fields[0].split(",")[1];
			String key2 = fields[0].split(",")[0].split(" ")[0]
					+ " "
					+ Integer.toString(Integer.parseInt(fields[0].split(",")[0]
							.split(" ")[1]) - 1) + "-"
					+ fields[0].split(",")[0].split(" ")[1] + "_"
					+ fields[0].split(",")[1];
			word.set(key1);
			valMap.set(fields[1]);
			context.write(word, valMap);
			word.set(key2);
			valMap.set(fields[1]);
			context.write(word, valMap);
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		private int flag1 = 0;
		private int flag2 = 0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			String[] temp1 = iterator.next().toString().split(",");
			float enr2 = Integer.parseInt(temp1[0]);
			float cap2 = Integer.parseInt(temp1[1]);
			if (!iterator.hasNext())
				return;
			String[] temp2 = iterator.next().toString().split(",");
			float enr1 = Integer.parseInt(temp2[0]);
			float cap1 = Integer.parseInt(temp2[1]);
			float delEnr = 0, delCap = 0;
			if (flag1 == 0) {
				delEnr = (enr2 - enr1) / (enr1) * 100;
				flag1 = 1;
			} else if (flag1 == 1) {
				delEnr = (enr1 - enr2) / (enr2) * 100;
				flag1 = 0;
			}

			if (flag2 == 0) {
				delCap = (cap2 - cap1) / (cap1) * 100;
				flag2 = 1;
			} else if (flag2 == 1) {
				delCap = (cap1 - cap2) / (cap2) * 100;
				flag2 = 0;
			}
			result.set(String.valueOf(delEnr) + "," + String.valueOf(delCap));
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp4";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,
				"gives the total enrollment,Capacity for every room in every building Semester-Year wise");
		job.setJarByClass(changeOverTheYears.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2,
				"gives the percentage change in the enrollment and capacity year on year for every room in every building for every semester");
		job2.setJarByClass(changeOverTheYears.class);
		job2.setMapperClass(Mapper2.class);
		// job2.setCombinerClass(FindIncreaseReducer.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(temp));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}
