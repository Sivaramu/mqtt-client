package com.optebiz.mqtt.mqttclient;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.util.StopWatch;

@SpringBootApplication
@ComponentScan(basePackageClasses = MqttClientConfig.class)
@EnableAsync
public class MqttClientApplication {

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
	private MqttSubscriber subscriber;

	private final int totalMessages = 1000000;

	public static Logger logger = LoggerFactory.getLogger(MqttClientApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(MqttClientApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			logger.info("MQTT Broker {}, Qos {}, Topic {}", mqttBroker, mqttQos, mqttTopic);

			String content = "Thanos! I am inevitable with id %d";

			int qos = Integer.valueOf(mqttQos);
			int maxInflight = Integer.valueOf(maxInflightProp);
			Semaphore semaphore = new Semaphore(maxInflight * 10);
			StopWatch watch = new StopWatch();
			MemoryPersistence persistence = new MemoryPersistence();
			try (MqttClient mqttClient = new MqttClient(mqttBroker, clientId, persistence);) {
				MqttConnectOptions connOpts = new MqttConnectOptions();
				connOpts.setCleanSession(true);
				connOpts.setMaxInflight(maxInflight);
				logger.info("Connecting to broker: {}", mqttBroker);

				mqttClient.connect(connOpts);
				subscriber.createSubscriber(mqttClient);
				logger.info("Client connected to the broker");
				mqttClient.setCallback(new MqttCallback() {
					@Override
					public void connectionLost(Throwable cause) {
						logger.warn("Connection is lost due to {}", cause.getMessage());
					}

					@Override
					public void messageArrived(String topic, MqttMessage message) throws Exception {
					}

					@Override
					public void deliveryComplete(IMqttDeliveryToken token) {
						semaphore.release();
					}
				});
				logger.info("Publishing messages");
				watch.start();
				for (int i = 0; i < totalMessages; i++) {
					MqttMessage message = new MqttMessage(String.format(content, i).getBytes());
					message.setId(i);
					message.setQos(qos);
					semaphore.acquire();
					mqttClient.publish(mqttTopic + "/device-" + (i % 10), message);
					if (i != 0 && i % 10000 == 0) {
						logger.info("Published {} messages", i);
					}
				}
				watch.stop();
				logger.info("Messages published in {} sec", watch.prettyPrint());
				MqttSubscriber.latch.await(30, TimeUnit.SECONDS);
				mqttClient.disconnect();
			} catch (MqttException me) {
				logger.error("Reason {}, Message {}, Localzied Message {}, Cause {}, Exception {}", me.getReasonCode(),
						me.getMessage(), me.getLocalizedMessage(), me.getCause(), me);
			}

		};
	}

}
