package org.apache.solr.store.blob.provider;

import org.apache.solr.common.SolrException;
import org.apache.solr.store.blob.client.BlobStorageClientBuilder;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.util.BlobStoreBootstrapper;

/**
 * Class that provides access to the shared storage client (blob client) and handles
 * initiation of such client. This class serves as the provider for all blob store
 * communication channels.
 * 
 * TODO - later stories should do the provisioning (setting up of auth, etc) for the 
 * client libraries via this class. 
 */
public class BlobStorageProvider {  
  /** 
   * The only blob that has a constant name for a core is the metadata. Basically the Blob store's equivalent for a core
   * of the highest segments_N file for a Solr server. 
   */
  public static final String CORE_METADATA_BLOB_FILENAME = "core.metadata";
  
  private CoreStorageClient storageClient;
  
  public CoreStorageClient getDefaultClient() {
    String localBlobDir = BlobStoreBootstrapper.getLocalBlobDir();
    String blobBucketName = BlobStoreBootstrapper.getBlobBucketName();
    String blobstoreEndpoint = BlobStoreBootstrapper.getBlobstoreEndpoint();
    String blobstoreAccessKey = BlobStoreBootstrapper.getBlobstoreAccessKey();
    String blobstoreSecretKey = BlobStoreBootstrapper.getBlobstoreSecretKey();
    String blobStorageProvider = BlobStoreBootstrapper.getBlobStorageProvider();
    return getClient(localBlobDir, blobBucketName, blobstoreEndpoint, 
        blobstoreAccessKey, blobstoreSecretKey, blobStorageProvider);
  }
  
  public CoreStorageClient getClient(String localBlobDir, String blobBucketName, String blobStoreEndpoint,
      String blobStoreAccessKey, String blobStoreSecretKey, String blobStorageProvider) {
    if (storageClient != null) {
      return storageClient;
    }
    
    try {
      BlobStorageClientBuilder clientBuilder = new BlobStorageClientBuilder(localBlobDir,
          blobStorageProvider, blobBucketName, blobStoreEndpoint, blobStoreAccessKey, blobStoreSecretKey
          );
      CoreStorageClient client = clientBuilder.build();
      // if we can't connect to the blob store for any reason, we'll throw an exception here
      boolean bucketExists = client.doesBucketExist();
      if (!bucketExists) {
        throw new Exception(
                String.format("The bucket %s does not exist! The CoreStorageClient will not connect to endpoint %s!",
             blobBucketName, blobStoreEndpoint));
      }
      storageClient = client;
      return client;
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not initiate new CoreStorageClient", ex);
    }
  }
}
