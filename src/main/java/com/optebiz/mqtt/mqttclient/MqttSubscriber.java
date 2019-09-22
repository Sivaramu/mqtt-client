package com.optebiz.mqtt.mqttclient;

import java.util.concurrent.CountDownLatch;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class MqttSubscriber {

	@Value("${mqtt.broker}")
	private String mqttBroker;

	@Value("${mqtt.qos}")
	private String mqttQos;

	@Value("${mqtt.topic}")
	private String mqttTopic;

	@Value("${mqtt.maxInflight}")
	private String maxInflightProp;

	@Value("${kafka.topic}")
	private String kafkaTopic;

	private String clientId = "mqttClient";

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	private static final int totalMessages = 1000000;

	public static final CountDownLatch latch = new CountDownLatch(totalMessages);

	public static Logger logger = LoggerFactory.getLogger(MqttSubscriber.class);

	@Async
	public void createSubscriber(MqttClient mqttClient) throws InterruptedException {

		int qos = Integer.valueOf(mqttQos);

		Runnable subscription = () -> {
			try {
				IMqttMessageListener listner = (t, message) -> {
					// logger.info("Message received => Topic {}, message={}", t, message);

					kafkaTemplate.send(kafkaTopic, message.toString()).addCallback((result) -> {
					}, (ex) -> {
						logger.error("send failed with error {}", ex.getMessage());
					});

					latch.countDown();
					long count = latch.getCount();
					if (count % 10000 == 0) {
						logger.info("Yet to consume {}", count);
					}
				};

				mqttClient.subscribe(mqttTopic + "/#", qos, listner);
			} catch (MqttException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		subscription.run();
	}
}
