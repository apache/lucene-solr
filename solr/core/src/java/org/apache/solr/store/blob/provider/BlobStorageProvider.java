package org.apache.solr.store.blob.provider;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.SolrException;
import org.apache.solr.store.blob.client.BlobException;
import org.apache.solr.store.blob.client.BlobStorageClientBuilder;
import org.apache.solr.store.blob.client.BlobstoreProviderType;
import org.apache.solr.store.blob.client.CoreStorageClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.SdkClientException;

/**
 * Class that provides access to the shared storage client (blob client) and
 * handles initiation of such client. This class serves as the provider for all
 * blob store communication channels.
 */
public class BlobStorageProvider {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CoreStorageClient storageClient;

  public CoreStorageClient getClient() {
    if (storageClient != null) {
      return storageClient;
    }

    return getClient(BlobstoreProviderType.getConfiguredProvider());
  }

  private synchronized CoreStorageClient getClient(BlobstoreProviderType blobStorageProviderType) {
    if (storageClient != null) {
      return storageClient;
    }

    try {
      log.info("CoreStorageClient: building CoreStorageClient for the first time. blobStorageProvider="
          + blobStorageProviderType.name());
      BlobStorageClientBuilder clientBuilder = new BlobStorageClientBuilder(blobStorageProviderType);
      CoreStorageClient client = clientBuilder.build();

      // if we can't connect to the blob store for any reason, we'll throw an
      // exception here
      boolean bucketExists = client.doesBucketExist();
      if (!bucketExists) {
        throw new BlobException(
            String.format("The bucket %s does not exist! The CoreStorageClient will not connect to endpoint %s!",
                client.getBucketName(), client.getEndpoint()));
      }
      storageClient = client;
      return client;

    } catch (IllegalArgumentException | IOException | BlobException | SdkClientException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not initiate new CoreStorageClient", ex);
    }
  }
}
