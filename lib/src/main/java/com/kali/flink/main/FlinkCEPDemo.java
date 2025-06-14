package com.kali.flink.main;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class FlinkCEPDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度为1

        long baseTime = System.currentTimeMillis();

        // 2. 创建模拟交易事件流（确保有连续三个大额交易）
        DataStream<Transaction> transactions = env.fromElements(
                new Transaction("user1", 1200, baseTime),
                new Transaction("user1", 1500, baseTime + 1000),
                new Transaction("user1", 2000, baseTime + 2000),
                new Transaction("user2", 3000, baseTime + 3000),
                new Transaction("user2", 1100, baseTime + 4000),
                new Transaction("user2", 1200, baseTime + 5000)
        );
//        transactions.print();
        transactions.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        // 2. 测试简单模式
        Pattern<Transaction, ?> testPattern = Pattern.<Transaction>begin("test")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction t) {
                        System.out.println("测试事件: " + t + " 金额>1000? " + (t.getAmount() > 1000));
                        return t.getAmount() > 1000;
                    }
                });

        SingleOutputStreamOperator<Object> test = CEP.pattern(transactions.keyBy(Transaction::getUserId), testPattern)
                .select(map -> "测试匹配: " + map.get("test").get(0));


        test.print();


        // 3. 定义CEP模式 - 连续3次大额交易
        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("first")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.getAmount() > 1000;
                    }
                })
                .next("second")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.getAmount() > 1000;
                    }
                })
                .next("third")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return transaction.getAmount() > 1000;
                    }
                })
                .within(Time.seconds(30)); // 增大时间窗口

        // 4. 将模式应用到流上
        PatternStream<Transaction> patternStream = CEP.pattern(
                transactions.keyBy(Transaction::getUserId), // 按用户ID分组
                pattern
        );

        // 5. 检测到模式时输出结果
        DataStream<String> result = patternStream.select((Map<String, List<Transaction>> patternMap) -> {
            Transaction first = patternMap.get("first").get(0);
            Transaction second = patternMap.get("second").get(0);
            Transaction third = patternMap.get("third").get(0);

            return String.format("警报! 用户 %s 连续三次大额交易: [%s, %s, %s]",
                    first.getUserId(),
                    first.getAmount(),
                    second.getAmount(),
                    third.getAmount());
        });

        // 6. 打印结果
        result.print();

        // 7. 执行程序
        env.execute("Flink CEP Demo");
    }

    // 交易事件POJO类（保持不变）
    public static class Transaction {
        private String userId;
        private double amount;
        private long timestamp;

        public Transaction() {
        }

        public Transaction(String userId, double amount, long timestamp) {
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public double getAmount() {
            return amount;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "userId='" + userId + '\'' +
                    ", amount=" + amount +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}