package br.com.proj.estudo.criminalidade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProjEstudoCriminalidadeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		Text _key = key;
		int cont = 0;
		
		for(IntWritable value: values) {
			cont += value.get();
		}
		context.write(_key, new IntWritable(cont));
	}

}
