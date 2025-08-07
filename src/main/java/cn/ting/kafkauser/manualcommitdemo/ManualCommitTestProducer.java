package cn.ting.kafkauser.manualcommitdemo;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ManualCommitTestProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private int messageCount = 0;

	public ManualCommitTestProducer(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Scheduled(fixedRate = 50) // 每500ms发送一条消息
	public void sendTestMessage() {
		String message = "测试消息-" + (++messageCount);
		kafkaTemplate.send("manual-commit-demo-topic", message);
		System.out.println("已发送: " + message);
	}
}
