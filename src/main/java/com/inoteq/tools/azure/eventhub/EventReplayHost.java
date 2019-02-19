package com.inoteq.tools.azure.eventhub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class EventReplayHost {
	private static final Logger LOG = LoggerFactory
			.getLogger(EventReplayHost.class);

	@Autowired
	private EventReplayExecutor replayHandler;

	public EventReplayHost(EventReplayExecutor replayHandler) {
		this.replayHandler = replayHandler;
	}

	public static void main(String args[]) {
		SpringApplication.run(EventReplayHost.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> start();
	}

	public void start() {
		try {
			replayHandler.replayEvents();

			System.exit(0);
		} catch (InterruptedException e) {
			LOG.error("Stopped replaying messages");

			System.exit(1);
		}
	}
}
