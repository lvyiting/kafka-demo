package cn.ting.kafkauser.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * @author : lvyiting
 * @date : 2025-05-02
 **/
@Service
public class KafkaMsg5 {

	@KafkaListener(topics = "sort-1", groupId = "group-sort",concurrency = "1")
	public void handleMessage(ConsumerRecord<?, ?> cus) {
		try {
			System.out.println("顺序消费"+cus.value());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@KafkaListener(topics = "sort-2", groupId = "group-sort",concurrency = "2")
	public void handleMessage1(ConsumerRecord<?, ?> cus) {
		try {
			System.out.println("不顺序消费"+cus.value());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
