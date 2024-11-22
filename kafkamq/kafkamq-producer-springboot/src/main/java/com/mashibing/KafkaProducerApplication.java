package com.mashibing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Hello world!
 */
@SpringBootApplication
public class KafkaProducerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class, args);
        logger.info("KafkaMQ Producer Launching!!!");
    }
}
