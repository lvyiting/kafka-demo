package cn.ting.kafkauser.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 测试 binlog 监听
 */
@Service
public class KafkaMsg {
    @KafkaListener(topics = "canal-test", groupId = "cache-group")
    public void handleMessage(ConsumerRecord<?, ?> cus) {
        try {
            System.out.println(cus);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
