package cn.ting.kafkauser.batchconsumedemo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : lvyiting
 * @date : 2025/08/07
 **/
@Slf4j
@Service
@RequiredArgsConstructor
public class AutoCommitBatchProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final AtomicInteger batchCounter = new AtomicInteger(0);

	/**
	 * 每2秒发送一批消息
	 */
	@Scheduled(fixedRate = 2000)
	public void sendBatchMessages() {
		int batchSize = 50 + (int)(Math.random() * 50); // 随机50-100条

		List<String> messages = new ArrayList<>();
		for (int i = 0; i < batchSize; i++) {
			int msgNum = batchCounter.incrementAndGet();
			messages.add("批量消息-" + msgNum);
		}

		// 批量发送
		for (String message : messages) {
			kafkaTemplate.send("auto-batch-topic", message);
		}

		log.info("<<<< 已发送批量消息: {}条", batchSize);
	}
}
