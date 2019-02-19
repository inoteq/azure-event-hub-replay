package com.inoteq.tools.azure.eventhub;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.models.BlobFlatListSegment;
import com.microsoft.azure.storage.blob.models.ListBlobsFlatSegmentResponse;

@Component
public class EventReplayExecutor {
	private static final Logger LOG = LoggerFactory
			.getLogger(EventReplayExecutor.class);

	private static final String PARTITION_PLACEHOLDER = "{PartitionId}";

	private final BlobStorageClient storageClient;
	private final ExecutorService executor;
	private final ApplicationContext context;

	@Autowired
	public EventReplayExecutor(ApplicationContext context,
			BlobStorageClient storageClient) {
		this.storageClient = storageClient;
		this.context = context;

		this.executor = Executors.newFixedThreadPool(storageClient
				.getStorageConfig().getThreads());
	}

	public void replayEvents() throws InterruptedException {
		long startTime = System.currentTimeMillis();

		for (int partitionIndex = 0; partitionIndex < storageClient
				.getStorageConfig().getNumPartitions(); partitionIndex++) {
			String partition = String.valueOf(partitionIndex);

			String path = storageClient.getStorageConfig().getRestorePath()
					.replace(PARTITION_PLACEHOLDER, partition);

			String marker = StringUtils.EMPTY;

			do {
				ListBlobsFlatSegmentResponse response = storageClient
						.getContainerUrl()
						.listBlobsFlatSegment(marker,
								new ListBlobsOptions().withPrefix(path))
						.blockingGet().body();
				marker = response.nextMarker();

				BlobFlatListSegment segment = response.segment();
				if (segment != null) {
					BlobItemHandler task = context
							.getBean(BlobItemHandler.class);
					task.setList(partition, segment.blobItems());
					executor.execute(task);
				} else {
					LOG.info("No blob items found for path {}", path);
				}
			} while (marker != null);
		}

		// Wait until all tasks are finished
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		long stopTime = (System.currentTimeMillis() - startTime) / 1000;

		LOG.info(
				"Finished replaying {} messages. {} events filtered. Elapsed time: {}s",
				BlobItemHandler.getEventsCount(),
				BlobItemHandler.getFilteredEventsCount(), stopTime);
	}
}
