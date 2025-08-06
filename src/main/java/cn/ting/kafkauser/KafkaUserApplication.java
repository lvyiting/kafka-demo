package cn.ting.kafkauser;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaUserApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaUserApplication.class, args);
    }
}
