package org.apache.solr.store.blob.client;

import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.google.common.collect.Iterables;

import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobClientUtils;
import org.apache.solr.store.blob.client.ToFromJson;

/**
 * This class implements an AmazonS3 client for reading and writing search index data to AWS S3.
 */
public class S3StorageClient implements CoreStorageClient {
    
    private final AmazonS3 s3Client;
    /** The key that identifies a file as the blob core metadata */
    private final String blobCoreMetadataName;
    /** The S3 bucket where we write all of our blobs to. */
    private String blobBucketName;
    /** The S3 endpoint this client will connect to. */
    private String endpoint;
    
    // S3 has a hard limit of 1000 keys per batch delete request
    private static final int MAX_KEYS_PER_BATCH_DELETE = 1000;
    
    /**
     * Construct a new S3StorageClient that is an implementation of the CoreStorageClient using AWS S3
     * as the underlying blob store service provider. 
     * 
     * @param blobBucketName the name of the bucket to write to
     * @param endpoint the endpoint for the aws s3 gateway
     * @param accessKey aws access key
     * @param secretKey aws secret
     */
    public S3StorageClient(String blobBucketName, String endpoint, String accessKey, String secretKey, String blobCoreMetadataName) {
        this.blobCoreMetadataName = blobCoreMetadataName;
        this.blobBucketName = blobBucketName;
        this.endpoint = endpoint;
          
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        s3Client = builder
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, /* signingRegion */ null))
                .build();
    }

    @Override
    public void pushCoreMetadata(String blobName, BlobCoreMetadata bcm) throws BlobException {
        try {
            ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
            String json = converter.toJson(bcm);
            
            String blobCoreMetadataPath = getBlobMetadataPath(blobName);
            /*
             * Encodes contents of the string into an S3 object. If no exception is thrown 
             * then the object is guaranteed to have been stored
             */
            s3Client.putObject(blobBucketName, blobCoreMetadataPath, json);
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public BlobCoreMetadata pullCoreMetadata(String blobName) throws BlobException {
        try {
            String blobCoreMetadataPath = getBlobMetadataPath(blobName);
            
            if (!coreMetadataExists(blobName)) {
                return null;
            }
             
            String decodedJson = s3Client.getObjectAsString(blobBucketName, blobCoreMetadataPath);
            ToFromJson<BlobCoreMetadata> converter = new ToFromJson<>();
            return converter.fromJson(decodedJson, BlobCoreMetadata.class);
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public InputStream pullStream(String path) throws BlobException {
        try {
            S3Object requestedObject = s3Client.getObject(blobBucketName, path);
            // This InputStream instance needs to be closed by the caller
            return requestedObject.getObjectContent();
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public String pushStream(String blobName, InputStream is, long contentLength, String fileNamePrefix) throws BlobException {
        try {
            /*
             * This object metadata is associated per blob. This is different than the Solr Core metadata 
             * {@link BlobCoreMetadata} which sits as a separate blob object in the store. At minimum, 
             * ObjectMetadata requires the content length of the object to be set in the request header.
             */
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(contentLength);
            
            String blobPath = BlobClientUtils.generateNewBlobCorePath(blobName, fileNamePrefix);
            PutObjectRequest putRequest = new PutObjectRequest(blobBucketName, blobPath, is, objectMetadata);
            
            s3Client.putObject(putRequest);
            is.close();
            return blobPath;
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public boolean coreMetadataExists(String blobName) throws BlobException {
        try {
            return s3Client.doesObjectExist(blobBucketName, getBlobMetadataPath(blobName));
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    private String getBlobMetadataPath(String blobName) {
        return BlobClientUtils.concatenatePaths(blobName, blobCoreMetadataName);
    }

    /**
     * Deletes all blob files associated with this blobName.
     * 
     * This is simply added here to handle cleaning up cores in the blob store for testing. Does not handle failed 
     * delete exceptions and no synchronization.
     * 
     * First sends a request to the BlobStore and gets a list of all blob file summaries 
     * prefixed by the given blobName. Gets the key for each blob and sends a delete request for all of those keys.
     */
    @Override
    public void deleteCore(String blobName) throws BlobException {
        try {
            ListObjectsRequest listRequest = new ListObjectsRequest();
            listRequest.setBucketName(blobBucketName);
            listRequest.setPrefix(blobName);
            
            List<String> blobFiles = new LinkedList<>();
            ObjectListing objectListing = s3Client.listObjects(listRequest);
            iterateObjectListingAndConsume(objectListing,  input -> {
                String key = input.getKey();
                blobFiles.add(key);
            });
            deleteObjects(blobFiles);
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    @Override
    public void deleteBlobs(Collection<String> paths) throws BlobException {
        try {
            /* 
             * Per the S3 docs: 
             * https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/DeleteObjectsResult.html
             * An exception is thrown if there's a client error processing the request or in
             * the Blob store itself. However there's no guarantee the delete did not happen 
             * if an exception is thrown. 
             */
            deleteObjects(paths);
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    @Override
    public BlobstoreProviderType getStorageProvider() {
        return BlobstoreProviderType.S3;
    }
    
    @Override 
    public String getBucketRegion() {
        // TODO: we'll just return the end point that our host is using for now as we don't know how service discovery will work per pod search cluster yet
        return endpoint;
    }
    
    @Override
    public String getBucketName() {
        return blobBucketName;
    }
    
    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public boolean doesBucketExist() throws BlobException {        
        try {
            return s3Client.doesBucketExistV2(blobBucketName);
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    @Override
    public void shutdown() {
        s3Client.shutdown();
    }
    
    @Override
    public List<String> listCoreBlobFilesOlderThan(String blobName, long timestamp) throws BlobException {
        ListObjectsRequest listRequest = new ListObjectsRequest();
        listRequest.setBucketName(blobBucketName);
        listRequest.setPrefix(blobName);
        
        List<String> blobFiles = new LinkedList<>();
        try {
            ObjectListing objectListing = s3Client.listObjects(listRequest);
            iterateObjectListingAndConsume(objectListing, object -> {
                long lastModifiedTimestamp = object.getLastModified().getTime();
                if (lastModifiedTimestamp < timestamp) {
                    blobFiles.add(object.getKey());
                }
            });
            return blobFiles;
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    @Override
    public List<String> listCommonBlobPrefix(String prefix) throws BlobException {
        ListObjectsRequest listRequest = new ListObjectsRequest();
        listRequest.setBucketName(blobBucketName);
        listRequest.setPrefix(prefix);
        listRequest.setDelimiter(BlobClientUtils.BLOB_FILE_PATH_DELIMITER);
        List<String> commonPrefixList = new LinkedList<>();
        try {
            ObjectListing objectListing = s3Client.listObjects(listRequest);
            
            while (true) {
                // strip the trailing delimiter character that gets appended to each string prefix
                List<String> prefixes = objectListing.getCommonPrefixes().stream()
                    .map(commonPrefix -> commonPrefix.substring(0, 
                            commonPrefix.length() - BlobClientUtils.BLOB_FILE_PATH_DELIMITER.length()))
                    .collect(Collectors.toList());
                
                commonPrefixList.addAll(prefixes);
                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            return commonPrefixList;
        } catch (AmazonServiceException ase) {
            throw handleAmazonServiceException(ase);
        } catch (AmazonClientException ace) {
            throw new BlobClientException(ace);
        } catch (Exception ex) {
            throw new BlobException(ex);
        }
    }
    
    private BlobException handleAmazonServiceException(AmazonServiceException ase) {
        String errMessage = String.format("An AmazonServiceException was thrown! [serviceName=%s] "
                + "[awsRequestId=%s] [httpStatus=%s] [s3ErrorCode=%s] [s3ErrorType=%s] [message=%s]", 
                ase.getServiceName(), ase.getRequestId(), ase.getStatusCode(),
                ase.getErrorCode(), ase.getErrorType(), ase.getErrorMessage());
        return new BlobServiceException(errMessage, ase);
    }
    
    // Note: we need to batch our keys into chunks for the s3 client, but the GCS client library
    // does this for us.
    private void deleteObjects(Collection<String> paths) {        
        List<DeleteObjectsRequest> deleteRequests = new LinkedList<>();
        
        // batch our deletes to MAX_KEYS_PER_BATCH_DELETE keys per request
        Iterables.partition(Iterables.transform(paths, p -> new KeyVersion(p)), MAX_KEYS_PER_BATCH_DELETE)
            .forEach(batch -> deleteRequests.add(createBatchDeleteRequest(batch)));
        
        for (DeleteObjectsRequest req : deleteRequests) {
            s3Client.deleteObjects(req);
        }
    }
    
    private DeleteObjectsRequest createBatchDeleteRequest(List<KeyVersion> keysToDelete) {
        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(blobBucketName);
        deleteRequest.setKeys(keysToDelete);
        return deleteRequest;
    }
    
    // helper to iterate object listings which may be truncated
    private void iterateObjectListingAndConsume(ObjectListing objectListing, Consumer<S3ObjectSummary> consumer) {
        while (true) {
            Iterator<S3ObjectSummary> iter = objectListing.getObjectSummaries().iterator();
            while (iter.hasNext()) {
                consumer.accept(iter.next());
            }
            
            if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }
    
}