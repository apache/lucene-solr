package org.apache.solr.store.blob.provider;

import search.blobstore.client.BlobStorageClientBuilder;
import search.blobstore.client.CoreStorageClient;
import searchserver.SfdcConfig;
import searchserver.SfdcConfigProperty;

/**
 * Provides SearchServer access to an external blob storage for Solr Cores by retrieving and bootstrapping storage clients with 
 * any necessary configurations or context. 
 *
 * @author a.vuong
 * @since 214/solr.6
 */
public class BlobStorageProvider {

    /** 
     * The only blob that has a constant name for a core is the metadata. Basically the Blob store's equivalent for a core
     * of the highest segments_N file for a Solr server. 
     */
    public static final String CORE_METADATA_BLOB_FILENAME = "core.metadata";
    private static BlobStorageProvider provider;
    
    private CoreStorageClient client;
    
    public static void init(SfdcConfig config) throws Exception {
        String localBlobDir = config.getSfdcConfigProperty(SfdcConfigProperty.LocalBlobStoreHome);
        String blobBucketName = config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceBucket);
        String blobstoreEndpoint = config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceEndpoint);
        String blobstoreAccessKey = config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceAccessToken);
        String blobstoreSecretKey = config.getSfdcConfigProperty(SfdcConfigProperty.BlobServiceSecretToken);
        String blobStorageProvider = config.getSfdcConfigProperty(SfdcConfigProperty.BlobStorageProvider);
        
        BlobStorageClientBuilder clientBuilder = new BlobStorageClientBuilder(localBlobDir,
                blobStorageProvider, blobBucketName, blobstoreEndpoint, blobstoreAccessKey, blobstoreSecretKey,
                CORE_METADATA_BLOB_FILENAME);
        CoreStorageClient client = clientBuilder.build();
        
        // if we can't connect to the blob store for any reason, we'll throw an exception here
        boolean bucketExists = client.doesBucketExist();
        if (!bucketExists) {
        throw new Exception(
                String.format("The bucket %s does not exist! The CoreStorageClient will not connect to endpoint %s!",
             blobBucketName, blobstoreEndpoint));
        }
        provider = new BlobStorageProvider(client);
    }
    
    public static BlobStorageProvider get() throws Exception {
        if (provider == null) {
            throw new Exception("BlobStorageProvider: Provider was not initialized. Quitting.");
        }
        return provider;
    }
    
    private BlobStorageProvider(CoreStorageClient client) {
        this.client = client;
    }
    
    /**
     * Returns the client used to push and pull from the blob store.
     */
    public synchronized CoreStorageClient getBlobStorageClient() throws Exception {
        return client;
    }
}