package cn.ting.kafkauser.manualcommitdemo;


import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 手动提交消费位移的Kafka消费者演示类
 *
 * 演示了如何在Spring Kafka中实现手动提交消费位移，
 * 并结合线程池处理消息以提高吞吐量，同时通过动态暂停和恢复分区
 * 来控制消费速率，防止消息积压或系统过载。
 *
 * 主要功能包括：
 * - 使用线程池异步处理消息
 * - 根据线程池队列使用情况动态暂停/恢复分区消费
 * - 手动提交消费位移确保消息处理完成后才更新消费进度
 * - 优雅关闭消费者和线程池
 */
@Component
public class ManualCommitConsumerDemo implements ConsumerSeekAware {

	// 线程池核心配置参数
	private final int corePoolSize = 4;
	private final int maxPoolSize = 8;
	private final int queueCapacity = 100;

	// 关闭标志位，用于优雅关闭
	private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

	// 记录当前被暂停的分区集合
	private final Set<TopicPartition> pausedPartitions = ConcurrentHashMap.newKeySet();

	// 消息处理线程池
	private ThreadPoolExecutor executorService;

	/**
	 * 用于模拟实际的消息处理逻辑
	 * 每条消息会随机延迟0-1秒处理
	 */
	private static class MessageProcessor {
		public void process(String message) {
			System.out.println("处理消息: " + message);
			// 模拟处理时间
			try {
				Thread.sleep((long) (Math.random() * 1000));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * 初始化线程池
	 * 在Spring容器初始化完成后自动调用
	 */
	@PostConstruct
	public void init() {
		this.executorService = new ThreadPoolExecutor(
				corePoolSize,
				maxPoolSize,
				60, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(queueCapacity),
				new ThreadPoolExecutor.CallerRunsPolicy()
		);
	}

	/**
	 * Kafka消息监听方法
	 * 监听"manual-commit-demo-topic"主题，使用手动提交位移模式
	 *
	 * @param consumer Kafka消费者实例
	 * @param record 接收到的消息记录
	 * @param acknowledgment 用于手动提交位移的对象
	 */
	@KafkaListener(
			topics = "manual-commit-demo-topic",
			groupId = "manual-commit-group",
			containerFactory = "manualCommitContainerFactory"
	)
	public void consume(Consumer<String, String> consumer,
	                    ConsumerRecord<String, String> record,
	                    Acknowledgment acknowledgment) {

		if (shuttingDown.get()) {
			return;
		}

		TopicPartition partition = new TopicPartition(record.topic(), record.partition());

		// 1. 检查队列容量，必要时暂停分区
		checkAndPausePartition(consumer, partition);

		// 2. 提交任务到线程池
		executorService.submit(() -> {
			try {
				// 消息处理
				processMessage(record.value());

				// 3. 手动提交位移
				acknowledgment.acknowledge();
				System.out.println("已提交位移: " + record.offset());

			} catch (Exception e) {
				System.err.println("处理消息失败: " + record.value() + ", 错误: " + e.getMessage());
			} finally {
				// 4. 尝试恢复分区消费
				tryResumePartition(consumer, partition);

				// 打印线程池状态
				printThreadPoolStatus();
			}
		});
	}

	/**
	 * 处理具体消息内容
	 * @param message 消息内容
	 */
	private void processMessage(String message) {
		new MessageProcessor().process(message);
	}

	/**
	 * 检查并根据线程池队列使用情况决定是否暂停分区消费
	 * 当队列使用率超过90%时暂停消费以防止内存溢出
	 *
	 * @param consumer Kafka消费者实例
	 * @param partition 当前分区
	 */
	private void checkAndPausePartition(Consumer<String, String> consumer,
	                                    TopicPartition partition) {
		// 当队列剩余容量小于10%时暂停分区消费
		int remainingCapacity = executorService.getQueue().remainingCapacity();
		int totalCapacity = executorService.getQueue().size() + remainingCapacity;
		double usedRatio = (double) executorService.getQueue().size() / totalCapacity;

		if (usedRatio > 0.2 && !pausedPartitions.contains(partition)) {
			pausedPartitions.add(partition);
			consumer.pause(Collections.singletonList(partition));
			System.out.println("已暂停分区: " + partition + " (队列使用率: " + (int)(usedRatio*100) + "%)");
		}
	}

	/**
	 * 尝试恢复之前暂停的分区消费
	 * 当队列使用率低于70%时恢复消费
	 *
	 * @param consumer Kafka消费者实例
	 * @param partition 当前分区
	 */
	private void tryResumePartition(Consumer<String, String> consumer,
	                                TopicPartition partition) {
		if (pausedPartitions.contains(partition)) {
			int remainingCapacity = executorService.getQueue().remainingCapacity();
			int totalCapacity = executorService.getQueue().size() + remainingCapacity;
			double usedRatio = (double) executorService.getQueue().size() / totalCapacity;

			if (usedRatio < 0.7) {
				pausedPartitions.remove(partition);
				consumer.resume(Collections.singletonList(partition));
				System.out.println("已恢复分区: " + partition + " (队列使用率: " + (int)(usedRatio*100) + "%)");
			}
		}
	}


	/**
	 * 打印线程池当前状态信息
	 * 包括活跃线程数、队列使用情况和暂停分区数量
	 */
	private void printThreadPoolStatus() {
		System.out.printf("线程池状态: [活跃线程: %d/%d, 队列: %d/%d, 暂停分区: %d]%n",
				executorService.getActiveCount(),
				executorService.getMaximumPoolSize(),
				executorService.getQueue().size(),
				executorService.getQueue().size() + executorService.getQueue().remainingCapacity(),
				pausedPartitions.size());
	}

	/**
	 * 优雅关闭消费者
	 * 在Spring容器销毁前自动调用
	 */
	@PreDestroy
	public void shutdown() {
		shuttingDown.set(true);
		executorService.shutdown();
		try {
			if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
				executorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			executorService.shutdownNow();
			Thread.currentThread().interrupt();
		}
		System.out.println("消费者已优雅关闭");
	}
}
