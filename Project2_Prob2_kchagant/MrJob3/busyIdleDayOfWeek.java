package MapReduce;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class busyIdleDayOfWeek {
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
			String partialKey = fields[1] + "," + fields[2].split(" ")[0];
			valMap.set("1");
			String day = fields[3];

			// In the case of M-F we need to assign count for all weekdays and
			// in M-S we need to include Saturday also
			if (day.contains("M-")) {
				word.set(partialKey + "," + "Monday");
				context.write(word, valMap);

				word.set(partialKey + "," + "Tuesday");
				context.write(word, valMap);

				word.set(partialKey + "," + "Wednesday");
				context.write(word, valMap);

				word.set(partialKey + "," + "Thursday");
				context.write(word, valMap);

				word.set(partialKey + "," + "Friday");
				context.write(word, valMap);

				if (day.equals("M-S")) {
					word.set(partialKey + "," + "Saturday");
					context.write(word, valMap);
				}
			}

			// Rest all cases we just check for occurrence of M,T,W,...
			else {
				if (day.contains("M")) {
					word.set(partialKey + "," + "Monday");
					context.write(word, valMap);
				}

				if (day.contains("T")) {
					word.set(partialKey + "," + "Tuesday");
					context.write(word, valMap);
				}
				if (day.contains("W")) {
					word.set(partialKey + "," + "Wednesday");
					context.write(word, valMap);
				}
				if (day.contains("R")) {
					word.set(partialKey + "," + "Thursday");
					context.write(word, valMap);
				}
				if (day.contains("F")) {
					word.set(partialKey + "," + "Friday");
					context.write(word, valMap);
				}
				if (day.contains("S")) {
					word.set(partialKey + "," + "Saturday");
					context.write(word, valMap);
				}
			}

		}

	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			result.set(String.valueOf(sum));
			context.write(key, result);

		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

		Text word = new Text();
		Text valMap = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] fields = value.toString().split("\\t");
			word.set(fields[0].split(",")[0] + "," + fields[0].split(",")[1]);
			valMap.set(fields[0].split(",")[2] + "," + fields[1]);
			context.write(word, valMap);
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int bookings = 0;
			String day = new String("");
			String temp1 = new String("");
			String temp2 = new String("");
			HashMap<String, Integer> map = new HashMap<String, Integer>();

			for (Text val : values) {
				day = val.toString().split(",")[0];
				bookings = Integer.parseInt(val.toString().split(",")[1]);
				map.put(day, bookings);
			}
			int maxValueInMap = (Collections.max(map.values()));
			for (Entry<String, Integer> entry : map.entrySet()) {
				if (entry.getValue() == maxValueInMap) {
					temp1 = entry.getKey() + "," + temp1;
				}
			}
			int minValueInMap = (Collections.min(map.values()));
			for (Entry<String, Integer> entry : map.entrySet()) {
				if (entry.getValue() == minValueInMap) {
					temp2 = entry.getKey() + "," + temp2;
				}
			}
			result.set("\t" + temp1 + String.valueOf(maxValueInMap) + "\t"
					+ temp2 + String.valueOf(minValueInMap));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp3";
		Configuration conf = new Configuration();
		Job job = Job
				.getInstance(
						conf,
						"gives the total number of reservations for classes on each day of the week in every building Semester-Year wise");
		job.setJarByClass(busyIdleDayOfWeek.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job
				.getInstance(
						conf2,
						"gives the Busiest and the most idle day of the week by number of reservations for every building Semester-Year Wise");
		job2.setJarByClass(busyIdleDayOfWeek.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(temp));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}