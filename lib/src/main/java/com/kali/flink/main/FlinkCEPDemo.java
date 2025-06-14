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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Flink CEP 演示：检测用户登录失败模式
 * 场景：连续3次登录失败的用户账户需要被锁定
 */
public class FlinkCEPDemo {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 创建登录事件数据流
        DataStream<LoginEvent> loginEventStream = env
                .addSource(new LoginEventSource())
                .assignTimestampsAndWatermarks(new LoginEventTimestampExtractor());

        // 按用户ID分组
        DataStream<LoginEvent> keyedStream = loginEventStream
                .keyBy(new KeySelector<LoginEvent, String>() {
                    @Override
                    public String getKey(LoginEvent event) {
                        return event.userId;
                    }
                });

        // 定义CEP模式：连续3次登录失败
        Pattern<LoginEvent, ?> loginFailPattern = Pattern
                .<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("FAIL");
                    }
                })
                .next("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("FAIL");
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("FAIL");
                    }
                })
                .within(Time.minutes(5)); // 5分钟内发生

        // 应用模式到数据流
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, loginFailPattern);

        // 处理匹配的模式
        DataStream<Alert> alerts = patternStream.select(
                new PatternSelectFunction<LoginEvent, Alert>() {
                    @Override
                    public Alert select(Map<String, List<LoginEvent>> pattern) {
                        LoginEvent first = pattern.get("first").get(0);
                        LoginEvent third = pattern.get("third").get(0);
                        
                        return new Alert(
                                first.userId,
                                "连续3次登录失败",
                                "账户可能被恶意攻击，建议锁定",
                                first.timestamp,
                                third.timestamp
                        );
                    }
                });

        // 输出告警信息
        alerts.map(new MapFunction<Alert, String>() {
            @Override
            public String map(Alert alert) {
                return String.format("🚨 安全告警: 用户[%s] %s，时间范围：%d - %d，建议：%s",
                        alert.userId, alert.message, alert.startTime, alert.endTime, alert.suggestion);
            }
        }).print();

        // 同时监控正常登录事件
        loginEventStream
                .filter(event -> event.status.equals("SUCCESS"))
                .map(event -> String.format("✅ 正常登录: 用户[%s] 在 %d 成功登录", event.userId, event.timestamp))
                .print();

        // 执行任务
        env.execute("Flink CEP Login Security Monitor");
    }

    // 登录事件类
    public static class LoginEvent {
        public String userId;
        public String status; // SUCCESS 或 FAIL
        public long timestamp;
        public String ip;

        public LoginEvent() {}

        public LoginEvent(String userId, String status, long timestamp, String ip) {
            this.userId = userId;
            this.status = status;
            this.timestamp = timestamp;
            this.ip = ip;
        }

        @Override
        public String toString() {
            return String.format("LoginEvent{userId='%s', status='%s', timestamp=%d, ip='%s'}",
                    userId, status, timestamp, ip);
        }
    }

    // 告警类
    public static class Alert {
        public String userId;
        public String message;
        public String suggestion;
        public long startTime;
        public long endTime;

        public Alert(String userId, String message, String suggestion, long startTime, long endTime) {
            this.userId = userId;
            this.message = message;
            this.suggestion = suggestion;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    // 模拟登录事件源
    public static class LoginEventSource implements SourceFunction<LoginEvent> {
        private boolean running = true;
        private Random random = new Random();
        private String[] users = {"user001", "user002", "user003", "admin", "guest"};
        private String[] ips = {"192.168.1.100", "192.168.1.101", "10.0.0.50", "172.16.0.10"};

        @Override
        public void run(SourceContext<LoginEvent> ctx) throws Exception {
            long currentTime = System.currentTimeMillis();
            
            while (running) {
                String userId = users[random.nextInt(users.length)];
                String ip = ips[random.nextInt(ips.length)];
                
                // 模拟登录状态，70%成功，30%失败
                String status = random.nextDouble() < 0.7 ? "SUCCESS" : "FAIL";
                
                // 对特定用户增加失败概率，模拟攻击场景
                if (userId.equals("admin") && random.nextDouble() < 0.8) {
                    status = "FAIL";
                }
                
                LoginEvent event = new LoginEvent(userId, status, currentTime, ip);
                ctx.collect(event);
                
                currentTime += random.nextInt(3000) + 1000; // 1-4秒间隔
                Thread.sleep(500); // 控制生成速度
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // CEP 更复杂的模式示例
    public static class AdvancedCEPPatterns {
        
        // 模式1：登录后立即进行高风险操作
        public static Pattern<LoginEvent, ?> getSuspiciousActivityPattern() {
            return Pattern.<LoginEvent>begin("login")
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return event.status.equals("SUCCESS");
                        }
                    })
                    .followedBy("suspicious")
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            // 模拟高风险操作检测逻辑
                            return event.ip.startsWith("10."); // 内网IP访问
                        }
                    })
                    .within(Time.minutes(2));
        }

        // 模式2：检测暴力破解攻击（同一IP多个账户失败）
        public static Pattern<LoginEvent, ?> getBruteForcePattern() {
            return Pattern.<LoginEvent>begin("failures")
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return event.status.equals("FAIL");
                        }
                    })
                    .timesOrMore(5) // 至少5次失败
                    .within(Time.minutes(3));
        }

        // 模式3：检测账户共享（同一账户不同IP快速切换）
        public static Pattern<LoginEvent, ?> getAccountSharingPattern() {
            return Pattern.<LoginEvent>begin("first_login")
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return event.status.equals("SUCCESS");
                        }
                    })
                    .followedBy("second_login")
                    .where(new SimpleCondition<LoginEvent>() {
                        @Override
                        public boolean filter(LoginEvent event) {
                            return event.status.equals("SUCCESS");
                        }
                    })
                    .within(Time.seconds(30)); // 30秒内从不同IP登录
        }
    }
}

// 时间戳提取器

class LoginEventTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<FlinkCEPDemo.LoginEvent> {
    
    public LoginEventTimestampExtractor() {
        super(Time.seconds(5)); // 最大延迟5秒
    }

    @Override
    public long extractTimestamp(FlinkCEPDemo.LoginEvent event) {
        return event.timestamp;
    }
}