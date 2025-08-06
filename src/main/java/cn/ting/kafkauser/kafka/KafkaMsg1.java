package cn.ting.kafkauser.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 测试
 * @author : lvyiting
 * @date : 2025-05-02
 **/
@Service
public class KafkaMsg1 {
	@KafkaListener(topics = "test-1", groupId = "cache-group")
	public void handleMessage(ConsumerRecord<?, ?> cus) {
		try {
			System.out.println(cus);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
