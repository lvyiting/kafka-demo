package cn.ting.kafkauser.batchconsumedemo;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AutoCommitBatchConsumer {

	/**
	 * 自动提交的批量消费者
	 *
	 * @param messages 批量消息列表
	 */
	@KafkaListener(
			topics = "auto-batch-topic",
			containerFactory = "autoCommitBatchContainerFactory"
	)
	public void consumeBatch(
			@Payload List<String> messages
	) {
		log.info(">>>> 收到批量消息: {}条", messages.size());

		// 模拟批量处理
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < messages.size(); i++) {
			String message = messages.get(i);
			log.info("处理消息: {}", message);

			// 模拟处理时间 (10-100ms)
			try {
				Thread.sleep((long) (Math.random() * 90 + 10));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		long duration = System.currentTimeMillis() - startTime;
		double avgTime = duration / (double) messages.size();

		log.info("批量处理完成: 总耗时={}ms, 平均={}ms/条", duration, avgTime);
	}
}
