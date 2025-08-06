package cn.ting.kafkauser;

import cn.ting.kafkauser.entity.User;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootTest
class KafkaUserApplicationTests {

    @Autowired
    private KafkaTemplate<String, String> stringKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, User> userKafkaTemplate;

    /**
     * 测试：发送消息
     */
    @Test
    public void sendMessage() {
        stringKafkaTemplate.send("test-1", "123");
    }

    /**
     * 同一个主题，不同的组，同一条消息会进入每一个不同的组
     */
    @Test
    public void send2() {
        stringKafkaTemplate.send("topic-1", "消息来啦~~~~~同一个主题，不同的组，同一条消息会进入每一个不同的组");
    }

    /**
     * 同一个主题，相同的组，在相同的组中同一条消息只会被一个组消费
     */
    @Test
    public void send3() {
        stringKafkaTemplate.send("topic-1", "消息来啦~~~~~同一个主题，相同的组，在相同的组中同条消息只被一个组消费");
    }

    /**
     * 设置concurrency = "1"，那么消息会被顺序消费
     */
    @Test
    public void send4() {
        stringKafkaTemplate.send("sort-1", "1");
        stringKafkaTemplate.send("sort-1", "2");
        stringKafkaTemplate.send("sort-1", "3");
        stringKafkaTemplate.send("sort-1", "4");
        stringKafkaTemplate.send("sort-1", "5");
        stringKafkaTemplate.send("sort-1", "6");
        stringKafkaTemplate.send("sort-1", "7");
        stringKafkaTemplate.send("sort-1", "8");
        stringKafkaTemplate.send("sort-1", "9");
        stringKafkaTemplate.send("sort-1", "10");
    }

    /**
     * 不顺序消费
     */
    @Test
    public void send5() {
        stringKafkaTemplate.send("sort-2", "1");
        stringKafkaTemplate.send("sort-2", "2");
        stringKafkaTemplate.send("sort-2", "3");
        stringKafkaTemplate.send("sort-2", "4");
        stringKafkaTemplate.send("sort-2", "5");
        stringKafkaTemplate.send("sort-2", "6");
        stringKafkaTemplate.send("sort-2", "7");
        stringKafkaTemplate.send("sort-2", "8");
        stringKafkaTemplate.send("sort-2", "9");
        stringKafkaTemplate.send("sort-2", "10");
    }

    /**
     * Header测试
     */
    @Test
    public void send6() {
        // 1. 创建 ProducerRecord
        String topic = "your-topic";
        String key = "user-1001";
        String value = "{\"action\": \"login\"}";

        // 2. 定义头信息
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("source", "mobile-app".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("priority", "high".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("tableName", "user-table".getBytes(StandardCharsets.UTF_8)));

        // 3. 创建 ProducerRecord 并附加头信息
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,
                key,
                value,
                headers
        );

        // 4. 发送消息
        stringKafkaTemplate.send(record);
    }

    /**
     * 发送实体类测试
     */
    @Test
    public void send7() {
        User user = new User();
        user.setName("ting");
        user.setAge(18);
        userKafkaTemplate.send("user-topic", user);
    }
}
