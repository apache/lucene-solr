package org.apache.solr.store.blob.client;

import java.io.IOException;

/**
 * Builder for {@link CoreStorageClient}
 */
public class BlobStorageClientBuilder {
  private BlobstoreProviderType blobStorageProvider;
  private static final String UNKNOWN_PROVIDER_TYPE = "Blob storage provider [%s] is unknown. Please check configuration.";

  public BlobStorageClientBuilder(BlobstoreProviderType blobStorageProvider) {
    this.blobStorageProvider = blobStorageProvider;
  }

  public CoreStorageClient build() throws IllegalArgumentException, IOException {
    CoreStorageClient client;

    switch (blobStorageProvider) {
      case LOCAL_FILE_SYSTEM:
        client = new LocalStorageClient();
        break;
      case S3:
        client = new S3StorageClient();
        break;
      default:
        throw new IllegalArgumentException(String.format(UNKNOWN_PROVIDER_TYPE, blobStorageProvider.name()));
    }
    return client;
  };
}
