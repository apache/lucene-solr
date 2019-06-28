package org.apache.solr.store.blob.client;
import org.apache.solr.store.blob.client.BlobstoreProviderType;
 
/**
 * Builder for {@link CoreStorageClient}
 */
public class BlobStorageClientBuilder {
    private String localStorageRootDir;
    private String blobBucketName;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String blobStorageProvider;
    private static final String UNKNOWN_PROVIDER_TYPE = "Blob storage provider [%s] is unknown. Please check configuration.";
     
     public BlobStorageClientBuilder(String localStorageRootDir, String blobStorageProvider,
            String blobBucketName, String endpoint, String accessKey, String secretKey) {
        this.localStorageRootDir = localStorageRootDir;
        this.blobBucketName = blobBucketName;
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.blobStorageProvider = blobStorageProvider;
    }
    
    public CoreStorageClient build() throws Exception {
        CoreStorageClient client;
        if (blobStorageProvider.equals(BlobstoreProviderType.LOCAL_FILE_SYSTEM.name())) {
            if (isNullOrEmpty(localStorageRootDir)) {
            throw new IllegalArgumentException(String.format(
                    "Could not build LocalStorageClient due to invalid fields! "
                        + "Displaying non-secret values for debug: localStorageDir=%s", localStorageRootDir));
            }
            client = new LocalStorageClient(localStorageRootDir);
        } else if (blobStorageProvider.equals(BlobstoreProviderType.S3.name())) {
            if (isNullOrEmpty(blobBucketName, endpoint, accessKey, secretKey)) {
            throw new IllegalArgumentException(String.format("Could not build S3StorageClient due to invalid fields! "
                        + "Displaying non-secret values for debug: blobBucketName=%s endpoint=%s ", blobBucketName, endpoint));
            }
            client = new S3StorageClient(blobBucketName, endpoint, accessKey, secretKey);
        } else {
            throw new IllegalArgumentException(String.format(UNKNOWN_PROVIDER_TYPE, blobStorageProvider));
        }
         return client;
    };
    
    private boolean isNullOrEmpty(String... values) {
        for (int i = 0; i < values.length; i++) {
            if (isNullOrEmpty(values[i])) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isNullOrEmpty(String value) {
        return (value == null) || value.isEmpty();
    }
}
