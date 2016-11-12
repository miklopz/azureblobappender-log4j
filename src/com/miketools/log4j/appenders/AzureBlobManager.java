package com.miketools.log4j.appenders;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public final class AzureBlobManager {

	private final String storageConnectionString;
	private final CloudStorageAccount cStorageAccount;
	private final CloudBlobContainer bContainer;
	private final CloudBlobClient blobClient;
	
	private final String name;
	
	public AzureBlobManager(String name, String storageAccount, String accountKey, String container, String endpointSuffix)
			throws InvalidKeyException, URISyntaxException, StorageException {
		
		storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + storageAccount + ";AccountKey=" + accountKey + ((endpointSuffix != null && endpointSuffix.trim().length() > 0) ? (";EndpointSuffix=" + endpointSuffix.trim()) : "");

		cStorageAccount = CloudStorageAccount.parse(storageConnectionString);
		blobClient = cStorageAccount.createCloudBlobClient();
		bContainer = blobClient.getContainerReference(container);
		bContainer.createIfNotExists();
		
		this.name = name;
	}
	
	public AzureBlobManager(String name, String storageAccount, String accountKey, String container) throws InvalidKeyException, URISyntaxException, StorageException {
		this(name, storageAccount, accountKey, container, "");
	}
	
	public synchronized void write(String message) throws URISyntaxException, StorageException, IOException, InterruptedException {
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, 1);
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		format1.setTimeZone(TimeZone.getTimeZone("UTC"));
		String formatted = getName() + format1.format(cal.getTime()) + ".log";
		
		CloudAppendBlob blob = bContainer.getAppendBlobReference(formatted);
		if(!blob.exists()) {
			blob.createOrReplace();
			// Probably worth writing headers here :)
		}
		
		// This may fail if the storage is deleted anyway...
		int i = 0;
		boolean done = false;
		StorageException lastException = null;
		while(i < 3 && !done) {
			try {
				blob.appendText(message);
				done = true;
			}
			catch(StorageException ex) {
				lastException = ex;
				if(ex.getHttpStatusCode() == 404) {
					if(!blob.exists()) {
						blob.createOrReplace();
					}
					Thread.sleep(2000 + (i*2000));
				}
			}
			i++;
		}
		
		if(i == 3)
			throw lastException;
	}

	public String getName() {
		return name;
	}
}
