package cn.ting.kafkauser.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaMsg6 {

	@KafkaListener(topics = "your-topic")
	public void listen(
			String payload,
			@Headers MessageHeaders headers,
			@Header(value = "tableName", required = false) String tableName
	) {
		log.info("Received message with payload: {}", payload);
		log.info("All headers: {}", headers);
		log.info("tableName header value: {}", tableName);
		
		if (tableName == null) {
			log.warn("Received message without tableName header. Payload: {}", payload);
			tableName = "unknown_table";
		}
		log.info("Processing data from table: {}", tableName);
		// 业务逻辑...
	}
}
