package com.inoteq.tools.azure.eventhub.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BlobStorageSourceConfig {
	private int numPartitions;
	private String restorePath;
	private String accountName;
	private String accountKey;
	private String containerName;
	private String logLevel;
	private int durationSlowMs;
	private int threads;
	private String regexFilter;

	public BlobStorageSourceConfig(
			@Value("${azure.storage.num_partitions}") int numPartitions,
			@Value("${azure.storage.restorePath}") String restorePath,
			@Value("${azure.storage.account.name}") String accountName,
			@Value("${azure.storage.account.key}") String accountKey,
			@Value("${azure.storage.container}") String containerName,
			@Value("${azure.storage.log.level}") String logLevel,
			@Value("${azure.storage.log.duration_slow_ms}") int durationSlowMs,
			@Value("${azure.storage.client.threads}") int threads,
			@Value("${azure.storage.event.body.filter_regex}") String regexFilter) {
		this.numPartitions = numPartitions;
		this.restorePath = restorePath;
		this.accountName = accountName;
		this.accountKey = accountKey;
		this.containerName = containerName;
		this.logLevel = logLevel;
		this.durationSlowMs = durationSlowMs;
		this.threads = threads;
		this.setRegexFilter(regexFilter);
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
	}

	public String getAccountName() {
		return accountName;
	}

	public void setAccountName(String accountName) {
		this.accountName = accountName;
	}

	public String getAccountKey() {
		return accountKey;
	}

	public void setAccountKey(String accountKey) {
		this.accountKey = accountKey;
	}

	public String getContainerName() {
		return containerName;
	}

	public void setContainerName(String containerName) {
		this.containerName = containerName;
	}

	public int getNumPartitions() {
		return numPartitions;
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}

	public String getRestorePath() {
		return restorePath;
	}

	public void setRestorePath(String restorePath) {
		this.restorePath = restorePath;
	}

	public int getDurationSlowMs() {
		return durationSlowMs;
	}

	public void setDurationSlowMs(int durationSlowMs) {
		this.durationSlowMs = durationSlowMs;
	}

	public String getRegexFilter() {
		return regexFilter;
	}

	public void setRegexFilter(String regexFilter) {
		this.regexFilter = regexFilter;
	}

}
