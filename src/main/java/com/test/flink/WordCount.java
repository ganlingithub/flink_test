package com.test.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class WordCount {
    private static Logger logger = LoggerFactory.getLogger(WordCount.class);
    public static void main(String[] args) throws Exception{
        //创建执行环境
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        String inputPath="D:\\ganlin10\\Desktop\\waterdrop\\flink_test\\src\\main\\resources\\hello.txt";
        env.readTextFile(inputPath);
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        //按空格分词
        DataSet<Tuple2<String,Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)//按第一个位置word分组
                .sum(1);//第二个位置sum求和
        resultSet.print();
        logger.info("日志");
    }
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{


        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words=value.split(" ");
            //遍历所有Word.包成二元组输出
            for(String word:words){
                out.collect(new Tuple2<>(word,1));
            }
        }


    }

}
