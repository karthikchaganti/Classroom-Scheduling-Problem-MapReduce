package MapReduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class top5EnrolledCoursesBySem {

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
    	word.set(fields[1] + "," + fields[6]);
    	valMap.set(fields[7]);
    	context.write(word, valMap);
    }
  }

  public static class Reducer1
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	int sumEnr=0;
    	for(Text val:values){
    		sumEnr+=Integer.parseInt(val.toString().split(",")[0]);
    	}
    	result.set(String.valueOf(sumEnr));
    	context.write(key,result);
    }
  }
  public static class Mapper2
  extends Mapper<Object, Text, Text, Text>{
  	private Text word = new Text();
  	private Text valMap = new Text();
public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	String[] fields = value.toString().split("\\t"); 
	word.set(fields[0].split(",")[0]);
	valMap.set(fields[0].split(",")[1] + "," + fields[1]);
	context.write(word,valMap);

}
}

public static class Reducer2
  extends Reducer<Text,Text,Text,Text> {
	private Text result = new Text();
public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	int enr=0;
	String course = new String("");
	HashMap<String,Integer> map = new HashMap<String,Integer>();
	for(Text val:values){
		course = val.toString().split(",")[0];
		enr=Integer.parseInt(val.toString().split(",")[1]);
		map.put(course,enr);
	}
	Map<String,Integer> sortedMap = new LinkedHashMap<String,Integer>();
	sortedMap = sortByValues(map);
	final Set<Entry<String,Integer>> mapValues = sortedMap.entrySet();
    final int maplength = mapValues.size();
    final Entry<String,Integer>[] test = new Entry[maplength];
    mapValues.toArray(test);
    
	result.set(test[maplength-1].getKey() + "," + test[maplength-1].getValue() + "," + test[maplength-2].getKey() + "," + test[maplength-2].getValue() 
			+ "," + test[maplength-3].getKey() + "," + test[maplength-3].getValue() + "," + test[maplength-4].getKey() + "," 
			+ test[maplength-4].getValue() + "," + "," + test[maplength-5].getKey() + "," + test[maplength-5].getValue());
	context.write(key,result);
}
public static <K extends Comparable<String>,V extends Comparable<Integer>> Map<K,V> sortByValues(Map<K,V> map){
    List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
  
    Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {

        @Override
        public int compare(Entry<K, V> o1, Entry<K, V> o2) {
            return o1.getValue().compareTo((Integer) o2.getValue());
        }
    });
  
    //LinkedHashMap will keep the keys in the order they are inserted
    //which is currently sorted on natural ordering
    Map<K,V> sortedMap = new LinkedHashMap<K,V>();
  
    for(Map.Entry<K,V> entry: entries){
        sortedMap.put(entry.getKey(), entry.getValue());
    }
  
    return sortedMap;
}
}
  public static void main(String[] args) throws Exception {
	String temp="Temp2";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "gives the total enrollment of each course Semester-Year wise");
    job.setJarByClass(top5EnrolledCoursesBySem.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(temp));
    job.waitForCompletion(true);
   
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "returns the top 5 higest enrolled courses every Semester-Year wise");
    job2.setJarByClass(top5EnrolledCoursesBySem.class);
    job2.setMapperClass(Mapper2.class);

    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(temp));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
