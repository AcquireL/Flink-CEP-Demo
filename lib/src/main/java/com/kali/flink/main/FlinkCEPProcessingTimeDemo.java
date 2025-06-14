package com.kali.flink.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Flink CEP 系统时间配置演示
 * 展示如何在 Flink CEP 中使用 Processing Time（系统时间）
 */
public class FlinkCEPProcessingTimeDemo {

    public static void main(String[] args) throws Exception {
        // 1. 设置执行环境，使用 Processing Time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 关键配置：设置时间特性为 Processing Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        System.out.println("🕒 Flink CEP 使用系统处理时间模式启动...");
        System.out.println("⏰ 当前系统时间: " + getCurrentTimeString());

        // 2. 创建交易事件数据流
        DataStream<TransactionEvent> transactionStream = env
                .addSource(new TransactionEventSource())
                .name("Transaction Source");

        // 3. 按用户ID分组
        DataStream<TransactionEvent> keyedStream = transactionStream
                .keyBy(new KeySelector<TransactionEvent, String>() {
                    @Override
                    public String getKey(TransactionEvent event) {
                        return event.userId;
                    }
                });

        // 4. 定义CEP模式：5分钟内连续3笔大额交易（基于系统时间）
        Pattern<TransactionEvent, ?> suspiciousPattern = Pattern
                .<TransactionEvent>begin("first")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.amount > 10000; // 大额交易
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.amount > 10000;
                    }
                })
                .followedBy("third")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.amount > 10000;
                    }
                })
                .within(Time.minutes(5)); // 5分钟窗口，基于系统处理时间

        // 5. 应用模式到数据流
        PatternStream<TransactionEvent> patternStream = CEP.pattern(keyedStream, suspiciousPattern);

        // 6. 处理匹配的模式
        DataStream<SuspiciousAlert> alerts = patternStream.select(
                new PatternSelectFunction<TransactionEvent, SuspiciousAlert>() {
                    @Override
                    public SuspiciousAlert select(Map<String, List<TransactionEvent>> pattern) {
                        List<TransactionEvent> first = pattern.get("first");
                        List<TransactionEvent> second = pattern.get("second");
                        List<TransactionEvent> third = pattern.get("third");
                        
                        double totalAmount = first.get(0).amount + second.get(0).amount + third.get(0).amount;
                        
                        return new SuspiciousAlert(
                                first.get(0).userId,
                                "连续大额交易警告",
                                totalAmount,
                                3,
                                System.currentTimeMillis(), // 使用当前系统时间
                                "基于处理时间检测"
                        );
                    }
                });

        // 7. 输出告警信息
        alerts.map(new MapFunction<SuspiciousAlert, String>() {
            @Override
            public String map(SuspiciousAlert alert) {
                return String.format("🚨 [%s] 可疑交易告警: 用户[%s] %s，总金额:%.2f，交易次数:%d，检测方式:%s",
                        getCurrentTimeString(),
                        alert.userId, 
                        alert.alertType, 
                        alert.totalAmount, 
                        alert.transactionCount,
                        alert.detectionMethod);
            }
        }).print("ALERT");

        // 8. 输出所有交易事件（带系统时间戳）
        transactionStream.map(new MapFunction<TransactionEvent, String>() {
            @Override
            public String map(TransactionEvent event) {
                return String.format("💰 [%s] 交易事件: 用户[%s] 金额:%.2f 类型:%s",
                        getCurrentTimeString(),
                        event.userId, 
                        event.amount, 
                        event.transactionType);
            }
        }).print("TRANSACTION");

        // 9. 演示不同的时间窗口配置
        demonstrateTimeWindowConfigurations(keyedStream);

        // 执行任务
        env.execute("Flink CEP Processing Time Demo");
    }

    // 演示不同时间窗口的配置
    private static void demonstrateTimeWindowConfigurations(DataStream<TransactionEvent> keyedStream) {
        
        // 配置1：短时间窗口 - 1分钟内2笔交易
        Pattern<TransactionEvent, ?> quickPattern = Pattern
                .<TransactionEvent>begin("quick1")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.amount > 5000;
                    }
                })
                .followedBy("quick2")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.amount > 5000;
                    }
                })
                .within(Time.minutes(1)); // 1分钟窗口

        PatternStream<TransactionEvent> quickPatternStream = CEP.pattern(keyedStream, quickPattern);
        quickPatternStream.select(new PatternSelectFunction<TransactionEvent, String>() {
            @Override
            public String select(Map<String, List<TransactionEvent>> pattern) {
                return String.format("⚡ [%s] 快速交易模式: 用户在1分钟内进行了2笔大额交易",
                        getCurrentTimeString());
            }
        }).print("QUICK_PATTERN");

        // 配置2：长时间窗口 - 30分钟内5笔交易
        Pattern<TransactionEvent, ?> longPattern = Pattern
                .<TransactionEvent>begin("long")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.amount > 1000;
                    }
                })
                .timesOrMore(5) // 至少5次
                .within(Time.minutes(30)); // 30分钟窗口

        PatternStream<TransactionEvent> longPatternStream = CEP.pattern(keyedStream, longPattern);
        longPatternStream.select(new PatternSelectFunction<TransactionEvent, String>() {
            @Override
            public String select(Map<String, List<TransactionEvent>> pattern) {
                return String.format("📊 [%s] 高频交易模式: 用户在30分钟内进行了%d笔交易",
                        getCurrentTimeString(), 
                        pattern.get("long").size());
            }
        }).print("HIGH_FREQUENCY");
    }

    // 获取当前时间字符串
    private static String getCurrentTimeString() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    // 交易事件类
    public static class TransactionEvent {
        public String userId;
        public double amount;
        public String transactionType;
        public long systemTimestamp; // 系统处理时间戳

        public TransactionEvent() {}

        public TransactionEvent(String userId, double amount, String transactionType) {
            this.userId = userId;
            this.amount = amount;
            this.transactionType = transactionType;
            this.systemTimestamp = System.currentTimeMillis(); // 记录系统时间
        }

        @Override
        public String toString() {
            return String.format("TransactionEvent{userId='%s', amount=%.2f, type='%s', systemTime=%d}",
                    userId, amount, transactionType, systemTimestamp);
        }
    }

    // 可疑交易告警类
    public static class SuspiciousAlert {
        public String userId;
        public String alertType;
        public double totalAmount;
        public int transactionCount;
        public long alertTime;
        public String detectionMethod;

        public SuspiciousAlert(String userId, String alertType, double totalAmount, 
                             int transactionCount, long alertTime, String detectionMethod) {
            this.userId = userId;
            this.alertType = alertType;
            this.totalAmount = totalAmount;
            this.transactionCount = transactionCount;
            this.alertTime = alertTime;
            this.detectionMethod = detectionMethod;
        }
    }

    // 交易事件源（基于系统时间生成）
    public static class TransactionEventSource implements SourceFunction<TransactionEvent> {
        private boolean running = true;
        private Random random = new Random();
        private String[] users = {"user_A", "user_B", "user_C", "user_D", "user_E"};
        private String[] transactionTypes = {"TRANSFER", "PAYMENT", "WITHDRAW", "DEPOSIT"};

        @Override
        public void run(SourceContext<TransactionEvent> ctx) throws Exception {
            System.out.println("🔄 交易事件源启动，开始生成基于系统时间的交易数据...");
            
            while (running) {
                String userId = users[random.nextInt(users.length)];
                String transactionType = transactionTypes[random.nextInt(transactionTypes.length)];
                
                // 生成不同金额的交易
                double amount;
                if (random.nextDouble() < 0.3) {
                    // 30% 概率生成大额交易
                    amount = 8000 + random.nextDouble() * 20000;
                } else {
                    // 70% 概率生成普通交易
                    amount = 100 + random.nextDouble() * 5000;
                }
                
                TransactionEvent event = new TransactionEvent(userId, amount, transactionType);
                
                // 使用系统当前时间进行收集
                ctx.collect(event);
                
                // 控制生成频率
                Thread.sleep(random.nextInt(2000) + 1000); // 1-3秒间隔
            }
        }

        @Override
        public void cancel() {
            running = false;
            System.out.println("⏹️ 交易事件源已停止");
        }
    }

    // Processing Time 配置工具类
    public static class ProcessingTimeConfig {
        
        /**
         * 配置环境使用处理时间
         */
        public static void configureProcessingTime(StreamExecutionEnvironment env) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            System.out.println("✅ 已配置 Flink 使用处理时间（Processing Time）");
        }
        
        /**
         * 打印时间配置信息
         */
        public static void printTimeConfiguration(StreamExecutionEnvironment env) {
            TimeCharacteristic timeCharacteristic = env.getStreamTimeCharacteristic();
            System.out.println("⚙️ 当前时间特性: " + timeCharacteristic);
            System.out.println("🕒 系统当前时间: " + System.currentTimeMillis());
            System.out.println("📅 格式化时间: " + getCurrentTimeString());
        }
        
        /**
         * 创建不同时间窗口的示例模式
         */
        public static void createTimeWindowExamples() {
            System.out.println("\n⏰ 时间窗口配置示例:");
            System.out.println("- Time.seconds(30): 30秒窗口");
            System.out.println("- Time.minutes(5):  5分钟窗口");
            System.out.println("- Time.hours(1):    1小时窗口");
            System.out.println("- Time.days(1):     1天窗口");
        }
    }
}