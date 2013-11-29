package easyTest;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import easyTest.WordCount.IntSumReducer;
import easyTest.WordCount.TokenizerMapper;

/*
 * The inputs are two files in the following format
 * table1.data:
Ruirui Lu SJTU Software Female table1
Xinni Ge SJTU Software Female table1
Biwen Li SJTU Software Female table1
Heng Lu SJTU Software Male table1
Yijing Qian SJTU Software Female table1
Tianxiang Miao SJTU Software Male table1
Biwen Li SJTU Software Male table1
Ruirui Lu SJTU Software Male table1
 * 
 * 
 * table2.data:
Ruirui Lu NJU Software Female table2
Yi Shi NJU Software Female table2
Yinghua Lu NJU Software Female table2
Biwen Li NJU Software Female table2
Heng Lu NJU Software Male table2
Tianxiang Miao NJU Software Male table2
Ruirui Lu NJU Software Male table2


 * Join op Join Two tables on student name
 *	The files can be merged into one for processing
 * */

public class JoinTwoTables {

	public static class StudentTableMapper extends Mapper<Object, Text, Text, Text> {

		private Text name = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			int secondSpace = line.indexOf(" ", line.indexOf(" ") + 1);
			String name = line.substring(0, secondSpace);
			String rest = line.substring(secondSpace + 1);
			context.write(new Text(name), new Text(rest));
		}
	}

	public static class TwoUniversityStudentJoinReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			ArrayList<String> table1Items=new ArrayList<String>();
			ArrayList<String> table2Items=new ArrayList<String>();
			for(Text info:values){
				String infoStr=info.toString();
				int index=infoStr.lastIndexOf(" ");
				String table=infoStr.substring(index+1);
				String otherInfo=infoStr.substring(0,index);
				if(table.equals("table1")){
					table1Items.add(otherInfo);
				}else if(table.equals("table2")){
					table2Items.add(otherInfo);
				}else{
					System.err.println("Table info wrong");
				}
			}
			
			for(String info1:table1Items){
				for(String info2:table2Items){
					context.write(key, new Text(info1+" "+info2));
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    String INPUTPATH="hdfs://localhost:9000/user/shelleyruirui/input/joinInput";
	    String OUTPUTPATH="hdfs://localhost:9000/user/shelleyruirui/output/joinOutput";
	    
	    Job job = new Job(conf, "table join");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(StudentTableMapper.class);
	    job.setReducerClass(TwoUniversityStudentJoinReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(INPUTPATH));
	    FileOutputFormat.setOutputPath(job, new Path(OUTPUTPATH));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	}
}
