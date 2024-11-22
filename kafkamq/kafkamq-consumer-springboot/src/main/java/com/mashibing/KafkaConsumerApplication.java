package com.mashibing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Hello world!
 */
@SpringBootApplication
public class KafkaConsumerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
        logger.info("KafkaMQ Consumer Launching!!!");
    }
}
