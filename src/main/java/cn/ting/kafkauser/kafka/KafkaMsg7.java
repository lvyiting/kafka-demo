package cn.ting.kafkauser.kafka;

import cn.ting.kafkauser.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 测试对象接收
 */
@Service
@Slf4j
public class KafkaMsg7 {

	@KafkaListener(topics = "user-topic", containerFactory = "userKafkaListenerContainerFactory")
	public void listen(User user) {
		log.info("Received user: {}", user);
	}
}
