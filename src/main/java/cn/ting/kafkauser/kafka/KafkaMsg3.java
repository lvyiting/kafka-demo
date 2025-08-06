package cn.ting.kafkauser.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * @author : lvyiting
 * @date : 2025-05-02
 **/
@Service
public class KafkaMsg3 {

	@KafkaListener(topics = "topic-1", groupId = "group-2")
	public void handleMessage(ConsumerRecord<?, ?> cus) {
		try {
			System.out.println("同一主题，不同消费组，消息分别进入消费组。group-2："+cus.value());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
