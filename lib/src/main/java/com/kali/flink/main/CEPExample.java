package com.kali.flink.main;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class CEPExample {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> source = (DataStreamSource<String>) env
                .addSource(new SocketTextStreamFunction("localhost", 9999, "\n", -1));

        DataStream<Tuple2<String, Integer>> stream =source.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0],Integer.parseInt(split[1]));
            }
        });
        stream.print();

        // 定义模式
        Pattern<Tuple2<String, Integer>, ?> pattern = Pattern.<Tuple2<String, Integer>>begin("start")
                .where(new SimpleCondition<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        System.out.println(value);
                        return value.f1 % 2 == 0;
                    }
                })
                .within(Time.seconds(20));


        // 应用模式匹配
        DataStream<String> result = CEP.pattern(stream, pattern)
                .select(new PatternSelectFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple2<String, Integer>>> pattern) throws Exception {
                        List<Tuple2<String, Integer>> startEvents = pattern.get("start");
                        return "Start: " + startEvents ;
                    }
                });
        // 输出结果
        result.print();

        // 执行作业
        env.execute("CEPExample");
    }
}
