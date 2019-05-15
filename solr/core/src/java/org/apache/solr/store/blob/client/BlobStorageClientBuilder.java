package org.apache.solr.store.blob.client;
 import org.apache.solr.store.blob.client.BlobstoreProviderType;
//import shaded.com.google.cloud.storage.BlobId;
//import shaded.com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
 /**
 * Builder for {@link CoreStorageClient}
 *
 * @author a.vuong
 * @since 218
 */
public class BlobStorageClientBuilder {
    private String localStorageRootDir;
    private String blobBucketName;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String blobCoreMetadataName;
    private String blobStorageProvider;
     private static final String UNKNOWN_PROVIDER_TYPE = "Blob storage provider [%s] is unknown. Please check configuration.";
     public BlobStorageClientBuilder(String localStorageRootDir, String blobStorageProvider,
            String blobBucketName, String endpoint, 
            String accessKey, String secretKey, String blobCoreMetadataName) {
        this.localStorageRootDir = localStorageRootDir;
        this.blobBucketName = blobBucketName;
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.blobCoreMetadataName = blobCoreMetadataName;
        this.blobStorageProvider = blobStorageProvider;
    }
    
    public CoreStorageClient build() throws Exception {
        CoreStorageClient client;
        if (blobStorageProvider.equals(BlobstoreProviderType.LOCAL_FILE_SYSTEM.name())) {
            if (isNullOrEmpty(localStorageRootDir)) {
            throw new IllegalArgumentException(String.format(
                    "Could not build LocalStorageClient due to invalid fields! "
                        + "Displaying non-secret values for debug: localStorageDir=%s blobCoreMetadataName=%s", localStorageRootDir, blobCoreMetadataName));
            }
            client = new LocalStorageClient(localStorageRootDir, blobCoreMetadataName);
//        } else if (blobStorageProvider.equals(BlobstoreProviderType.GCS.name())) {
//            if (isNullOrEmpty(blobBucketName, blobCoreMetadataName)) {
//            throw new IllegalArgumentException(String.format(
//                    "Could not build GCSStorageClient due to invalid fields! "
//                            + "Displaying non-secret values for debug: blobBucketName=%s " + "blobCoreMetadataName=%s",
//                    blobBucketName, blobCoreMetadataName)); }
//            if (!isNullOrEmpty(endpoint)
//                    && endpoint.equals(GCSStorageClient.LOCAL_STORAGE_ENDPOINT)) { return getGCSClientForTesting(); }
//            client = new GCSStorageClient(blobBucketName, blobCoreMetadataName);
        } else if (blobStorageProvider.equals(BlobstoreProviderType.S3.name())) {
            if (isNullOrEmpty(blobBucketName, endpoint, accessKey, secretKey,
                    blobCoreMetadataName)) {
            throw new IllegalArgumentException(String.format("Could not build S3StorageClient due to invalid fields! "
                        + "Displaying non-secret values for debug: blobBucketName=%s endpoint=%s "
                        + "blobCoreMetadataName=%s", blobBucketName, endpoint, blobCoreMetadataName));
            }
            client = new S3StorageClient(blobBucketName, endpoint, accessKey, secretKey, blobCoreMetadataName);
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
//
//    /**
//     * Creates a GCS Storage Client to be used while testing. Google's LocalStorageHelper uses FakeStorageRpc which has
//     * limited functionality, so we need to override certain methods.
//     */
//    private CoreStorageClient getGCSClientForTesting() throws BlobException {
//        return new GCSStorageClient(blobBucketName, endpoint,
//                BlobStorageProvider.CORE_METADATA_BLOB_FILENAME, LocalStorageHelper.getOptions().getService()) {
//            // The local Storage service cannot delete in batches
//            @Override
//            public void deleteBlobs(Collection<String> paths) throws BlobException {
//                for (String path : paths) {
//                    BlobId blobId = BlobId.of(getBucketName(), path);
//                    getStorage().delete(blobId);
//                }
//            }
//
//            // The local Storage service does not connect with an actual GCS bucket
//            @Override
//            public boolean doesBucketExist() throws BlobException {
//                return true;
//            }
//        };
//    }
}
