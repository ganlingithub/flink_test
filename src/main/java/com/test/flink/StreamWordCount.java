package com.test.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(8);
        env.setParallelism(1);
        ParameterTool parameterTool=ParameterTool.fromArgs(args);
        String host=parameterTool.get("host");
        int port=parameterTool.getInt("port");
        //String inputPath="D:\\ganlin10\\Desktop\\waterdrop\\flink_test\\src\\main\\resources\\hello.txt";
        //env.readTextFile(inputPath);
        //DataStream<String> inputDataStream = env.readTextFile(inputPath);
        DataStream<String> inputDataStream=env.socketTextStream(host,port);
        //基于流转换
        DataStream<Tuple2<String,Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();
        env.execute();
    }
}
