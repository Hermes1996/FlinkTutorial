package com.study.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


//批处理,每行单词次数统计
public class WordCounter
{
	public static void main(String[] args) throws Exception
	{
		//创建环境
		ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
		//读取数据
		String inputPath = "E:\\FlinkTutorial\\src\\main\\resources\\data";
		DataSource<String> stringDataSource = env.readTextFile(inputPath);//父类DataSet

		AggregateOperator<Tuple2<String, Integer>> wordCounter = stringDataSource.flatMap(new MyFlatMapper()).groupBy(0)
				.sum(1);//元祖左元素分组，右边1求和
		wordCounter.print();
	}

	public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{//泛型<T,O>输入,输出
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
		{
			//空格分词，转成（xx，1）scala二元祖
			String[] words =value.split(" ");
			for (String word : words)
			{
				out.collect(new Tuple2<>(word,1));
			}
		}
	}
}
