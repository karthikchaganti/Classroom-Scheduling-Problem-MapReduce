package MapReduce;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgEnrol {

  public static class Mapper1
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text valMap = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] fields=value.toString().split(","); //new array of 9 elements
    	if(fields[1].equals("Unknown") || fields[2].equals("Unknown") ||fields[2].equals("Arr Arr") || fields[3].equals("UNKWN") || fields[3].equals("ARR") 
       			|| fields[4].equals("Unknown") || fields[4].equals("Before 8:00AM") 
       			|| fields.length !=9 || Integer.parseInt(fields[7]) <= 0 ||Integer.parseInt(fields[8]) <= 0 
       			|| Integer.parseInt(fields[7]) > Integer.parseInt(fields[8])
       			||!StringUtils.isNumeric(fields[7]))
       		{ 
       		return; 
       		}
    	word.set(fields[1] + "," + fields[2]);
    	valMap.set(fields[7] + "," + "1");
    	context.write(word, valMap);
    }
  }

  public static class Reducer1
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sumEnr = 0;
			int sumCount = 0;
			for (Text val : values) {
				sumEnr += Integer.parseInt(val.toString().split(",")[0]);
				sumCount += Integer.parseInt(val.toString().split(",")[1]);
			}

			result.set(String.valueOf(sumEnr) + "," + String.valueOf(sumCount));
			context.write(key, result);
		}
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, Text>{
  	private Text word = new Text();
  	private Text valMap = new Text();
public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields = value.toString().split("\\t"); 
	word.set(fields[0].split(",")[0] + "," + fields[0].split(",")[1].split(" ")[0]);
	valMap.set(fields[1]);
	context.write(word,valMap);

}
}

public static class Reducer2
  extends Reducer<Text,Text,Text,Text> {
	private Text result = new Text();
public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	int sumEnr = 0;
	int sumCount = 0;
	for (Text val : values) {
		sumEnr += Integer.parseInt(val.toString().split(",")[0]);
		sumCount += Integer.parseInt(val.toString().split(",")[1]);
	}
	float avg =(float) sumEnr/sumCount;
	result.set(String.valueOf(avg));
	context.write(key, result);
}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp5";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Gives Total Enrollment for every room in every building semester-Year wise with the Count which will be used in the second MR job for calculating average");
    job.setJarByClass(AvgEnrol.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
   
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "gives the Avergae Enrollment for every building semester-Year Wise computed by dividing the summed Enrollments with the summed Count");
    job2.setJarByClass(AvgEnrol.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(temp));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
