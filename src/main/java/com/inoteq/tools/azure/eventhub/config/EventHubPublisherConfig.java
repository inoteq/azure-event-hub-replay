package com.inoteq.tools.azure.eventhub.config;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.RetryPolicy;

@Configuration
public class EventHubPublisherConfig {
	private String conStr;
	private int noThreads;
	private boolean readProperties;
	private boolean usePartitionSenderBatch;
	private boolean usePartitionKey;
	private int eventHubBatchSize;

	public EventHubPublisherConfig(
			@Value("${publish.eh.con_string1}") String conStr,
			@Value("${publish.eh.threads}") int noThreads,
			@Value("${publish.eh.event.read_properties}") boolean readProperties,
			@Value("${publish.eh.event.use_partition_key}") boolean usePartitionKey,
			@Value("${publish.eh.event.use_partition_sender_batch}") boolean usePartitionSenderBatch,
			@Value("${publish.eh.event.batch_size}") int eventHubBatchSize) {
		this.conStr = conStr;
		this.noThreads = noThreads;
		this.readProperties = readProperties;
		this.usePartitionKey = usePartitionKey;
		this.usePartitionSenderBatch = usePartitionSenderBatch;
		this.eventHubBatchSize = eventHubBatchSize;
	}

	@Bean
	public EventHubClient eventHubClient() throws EventHubException,
			IOException {
		ExecutorService executorService = Executors
				.newFixedThreadPool(noThreads);
		return EventHubClient.createSync(conStr, RetryPolicy.getDefault(),
				executorService);
	}

	public boolean readProperties() {
		return readProperties;
	}

	public void setReadProperties(boolean readProperties) {
		this.readProperties = readProperties;
	}

	public boolean usePartitionKey() {
		return usePartitionKey;
	}

	public void setUsePartitionKey(boolean usePartitionKey) {
		this.usePartitionKey = usePartitionKey;
	}

	public int getEventHubBatchSize() {
		return eventHubBatchSize;
	}

	public void setEventHubBatchSize(int eventHubBatchSize) {
		this.eventHubBatchSize = eventHubBatchSize;
	}

	public boolean usePartitionSenderBatch() {
		return usePartitionSenderBatch;
	}
}
