package br.com.proj.estudo.criminalidade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ProjEstudoCriminalidadeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final IntWritable ONE = new IntWritable(1);
	
	
	@Override
	public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException{
		
		String coluna = value.toString();
		String[] linhaRegistroBO = coluna.split(";");
		//Cidade que ocorreu o BO
		context.write(new Text(linhaRegistroBO[17]), ONE);
		
		//Mês que ocorreu o BO
		//context.write(new Text(linhaRegistroBO[10]), ONE);
		
		//Motivo do Boletim de Ocorrência
		//context.write(new Text(linhaRegistroBO[12]), ONE);
	}

}
