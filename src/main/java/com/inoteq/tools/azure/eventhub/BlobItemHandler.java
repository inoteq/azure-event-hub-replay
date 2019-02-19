package com.inoteq.tools.azure.eventhub;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.inoteq.tools.azure.eventhub.config.EventHubPublisherConfig;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.eventhubs.impl.AmqpConstants;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.DownloadResponse;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.rest.v2.util.FlowableUtil;

import avro.shaded.com.google.common.collect.Lists;

@Component
@Scope(SCOPE_PROTOTYPE)
public class BlobItemHandler implements Runnable {
	private static final Logger LOG = LoggerFactory
			.getLogger(BlobItemHandler.class);

	private static final String PARTITION_KEY = "partition-key-prop";

	private EventHubClient eventHubClient;
	private BlobStorageClient storageClient;
	private List<BlobItem> blobItems;
	private EventHubPublisherConfig eventHubConfig;
	private PartitionSender partitionSender;

	private static int filteredEventsCount;
	private static int eventsCount;

	@Autowired
	public BlobItemHandler(EventHubClient eventHubClient,
			BlobStorageClient storageClient,
			EventHubPublisherConfig eventHubConfig) {
		this.eventHubClient = eventHubClient;
		this.storageClient = storageClient;
		this.eventHubConfig = eventHubConfig;
	}

	@Override
	public void run() {
		for (BlobItem blobItem : blobItems) {
			BlockBlobURL blobURL = storageClient.getContainerUrl()
					.createBlockBlobURL(blobItem.name());

			DownloadResponse response = blobURL.download().blockingGet();

			ByteBuffer buffer = FlowableUtil.collectBytesInBuffer(
					response.body(null)).blockingGet();

			List<EventData> events = getEventDataFromAvroFile(buffer.array());

			// Empty buffer
			buffer.clear();

			if (events != null) {
				// Create batch files
				List<List<EventData>> batches = Lists.partition(events,
						eventHubConfig.getEventHubBatchSize());

				LOG.debug("Create {} event batch files", batches.size());

				batches.forEach(eventList -> {
					if (eventHubConfig.usePartitionKey()) {
						LOG.info(
								"Sending {} events to Event Hub reading the partition key {}",
								eventList.size(),
								eventHubClient.getEventHubName());

						// Iterate through all events and read the partition key
						for (EventData event : eventList) {
							if (event.getProperties()
									.containsKey(PARTITION_KEY)) {
								// Read partition key
								String partitionKey = (String) event
										.getProperties().get(PARTITION_KEY);

								// Remove temporary partition key from
								// properties
								event.getProperties().remove(PARTITION_KEY);

								try {
									// Send event to Event Hub with partition
									// key
									eventHubClient
											.sendSync(event, partitionKey);

									eventsCount++;
								} catch (EventHubException e) {
									LOG.error(
											"Error sending blob {} to Event Hub {}",
											blobItem.name(),
											eventHubClient.getEventHubName());
								}
							} else {
								try {
									// Send event to Event Hub
									eventHubClient.sendSync(event);

									eventsCount++;
								} catch (EventHubException e) {
									LOG.error(
											"Error sending blob {} to Event Hub {}",
											blobItem.name(),
											eventHubClient.getEventHubName());
								}
							}
						}
					} else {
						try {
							if (partitionSender != null) {
								partitionSender.sendSync(eventList);

								LOG.info(
										"Sending {} events to Event Hub {} with partition {}",
										eventList.size(),
										eventHubClient.getEventHubName(),
										partitionSender.getPartitionId());
							} else {
								eventHubClient.sendSync(eventList);

								LOG.info("Sending {} events to Event Hub {}",
										eventList.size(),
										eventHubClient.getEventHubName());
							}

							eventsCount += eventList.size();
						} catch (EventHubException e) {
							LOG.error("Error sending blob {} to Event Hub {}",
									blobItem.name(),
									eventHubClient.getEventHubName());
						}
					}
				});
			} else {
				LOG.info("Empty Avro file for blob {}", blobItem.name());
			}
		}
	}

	public void setList(String partition, List<BlobItem> blobItems) {
		this.blobItems = blobItems;

		if (eventHubConfig.usePartitionSenderBatch()) {
			try {
				this.partitionSender = eventHubClient
						.createPartitionSenderSync(partition);
			} catch (IllegalArgumentException | EventHubException e) {
				LOG.error("Error creating a partition sender for partition {}",
						partition);
			}
		}
	}

	private boolean keepEvent(String regex, byte[] eventBody) {
		String body = new String(eventBody, Charsets.UTF_8);
		if (Strings.isNullOrEmpty(regex)
				| (body != null && body.matches(regex))) {
			return true;
		} else {
			return false;
		}
	}

	private List<EventData> getEventDataFromAvroFile(byte[] bytes) {
		List<EventData> events = new ArrayList<>();
		DataFileReader<GenericRecord> reader = null;
		try {
			reader = new DataFileReader<>(new SeekableByteArrayInput(bytes),
					new GenericDatumReader<GenericRecord>());

			Iterator<GenericRecord> eventIterator = reader.iterator();
			while (eventIterator.hasNext()) {
				GenericRecord record = eventIterator.next();

				ByteBuffer body = (ByteBuffer) record.get("Body");

				String regexFilter = this.storageClient.getStorageConfig()
						.getRegexFilter();
				if (!keepEvent(regexFilter, body.array())) {
					filteredEventsCount++;

					// Continue with next event
					continue;
				}

				EventData eventData = EventData.create(body);

				body.clear();

				// Set properties if they exist and read properties option is
				// set
				if (eventHubConfig.readProperties()) {
					Map<String, Object> properties = getPropertiesFromRecord(record);
					if (!properties.isEmpty()) {
						eventData.getProperties().putAll(properties);
					}
				}

				// Set a partition key if available
				if (eventHubConfig.usePartitionKey()) {
					String partitionKey = getPartitionKeyFromRecord(record);
					if (partitionKey != null) {
						// Temporary save partition key to properties because we
						// cannot set system properties
						eventData.getProperties().put(PARTITION_KEY,
								partitionKey);
					}
				}

				events.add(eventData);
			}
			reader.close();
		} catch (IOException e) {
			LOG.error("Error parsing event");
			return null;
		}

		return events;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getPropertiesFromRecord(GenericRecord record) {
		return (Map<String, Object>) record.get("Properties");
	}

	private String getPartitionKeyFromRecord(GenericRecord record) {
		Map<?, ?> map = (Map<?, ?>) record.get("SystemProperties");

		Utf8 key = new Utf8(AmqpConstants.PARTITION_KEY.toString());

		if (map.containsKey(key)) {
			return map.get(key).toString();
		} else {
			return null;
		}
	}

	public static int getFilteredEventsCount() {
		return filteredEventsCount;
	}

	public static int getEventsCount() {
		return eventsCount;
	}
}
