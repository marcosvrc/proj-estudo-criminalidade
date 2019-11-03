package br.com.proj.estudo.criminalidade;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProjEstudoCriminalidade extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		if(arg0.length != 3) {
			System.err.printf("Usage : %s [generic options] <input> <output> \n", getClass().getSimpleName());
			
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration conf = this.getConf();
		Job job = new Job(conf);
		job.setJarByClass(ProjEstudoCriminalidade.class);
		job.setJobName("ProEstudoCriminalidade");
		FileInputFormat.addInputPath(job, new Path(arg0[1]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		job.setMapperClass(ProjEstudoCriminalidadeMapper.class);
		job.setReducerClass(ProjEstudoCriminalidadeReducer.class);
		job.setCombinerClass(ProjEstudoCriminalidadeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ProjEstudoCriminalidade(), args);
		System.exit(exitCode);
	}

}
