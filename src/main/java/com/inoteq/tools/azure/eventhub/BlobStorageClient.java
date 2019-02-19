package com.inoteq.tools.azure.eventhub;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.inoteq.tools.azure.eventhub.config.BlobStorageSourceConfig;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.LoggingOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.HttpPipelineLogger;

@Component
public class BlobStorageClient {
	private static final Logger LOG = LoggerFactory
			.getLogger(BlobStorageClient.class);

	private static final String AZURE_BLOB_STORAGE_URL = "https://%s.blob.core.windows.net";

	private BlobStorageSourceConfig storageConfig;
	private ContainerURL containerUrl;

	@Autowired
	public BlobStorageClient(BlobStorageSourceConfig storageConfig)
			throws InvalidKeyException, MalformedURLException {
		this.storageConfig = storageConfig;

		SharedKeyCredentials credentials = new SharedKeyCredentials(
				storageConfig.getAccountName(), storageConfig.getAccountKey());

		PipelineOptions po = new PipelineOptions();

		// Log a warning if requests need more than 2s
		po.withLoggingOptions(new LoggingOptions(storageConfig
				.getDurationSlowMs()));

		po.withLogger(new HttpPipelineLogger() {
			@Override
			public HttpPipelineLogLevel minimumLogLevel() {
				return HttpPipelineLogLevel.valueOf(storageConfig.getLogLevel());
			}

			@Override
			public void log(HttpPipelineLogLevel httpPipelineLogLevel,
					String s, Object... objects) {
				if (httpPipelineLogLevel == HttpPipelineLogLevel.ERROR) {
					LOG.error(s);
				} else if (httpPipelineLogLevel == HttpPipelineLogLevel.WARNING) {
					LOG.warn(s);
				} else if (httpPipelineLogLevel == HttpPipelineLogLevel.INFO) {
					LOG.info(s);
				}
			}
		});

		HttpPipeline pipeline = StorageURL.createPipeline(credentials, po);

		URL storageUrl = new URL(String.format(Locale.ROOT,
				AZURE_BLOB_STORAGE_URL, storageConfig.getAccountName()));

		ServiceURL serviceURL = new ServiceURL(storageUrl, pipeline);
		containerUrl = serviceURL.createContainerURL(storageConfig
				.getContainerName());
	}

	public BlobStorageSourceConfig getStorageConfig() {
		return storageConfig;
	}

	public ContainerURL getContainerUrl() {
		return containerUrl;
	}
}
