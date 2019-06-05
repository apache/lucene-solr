package org.apache.solr.store.blob.client;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;

 /**
 * Interface responsible for mapping Solr abstractions to an external blob store. It handles writing
 * to and reading Solr Cores from the blob store.
 */
public interface CoreStorageClient {
    
    /**
     * Replaces the special CORE_METADATA_BLOB_FILENAME blob on the blob store for the core by a new version passed as a
     * {@link BlobCoreMetadata} instance.
     * 
     * @param blobName name of the core to write metadata for
     * @param bcm blob metadata to be serialized and written to the blob store
     */
    public void pushCoreMetadata(String blobName, BlobCoreMetadata bcm) throws BlobException;
    
    /**
     * Reads the special CORE_METADATA_BLOB_FILENAME blob on the blob store for the core and returns the corresponding
     * {@link BlobCoreMetadata} object.
     * 
     * @param blobName name of the core to get metadata for
     * @return <code>null</code> if the core does not exist on the Blob store or method {@link #pushCoreMetadata} was
     * never called for it. Otherwise returns the latest value written using {@link #pushCoreMetadata} ("latest" here
     * based on the consistency model of the underlying store, in practice the last value written by any server given the
     * strong consistency of the Salesforce S3 implementation).
     */ 
    public BlobCoreMetadata pullCoreMetadata(String blobName) throws BlobException;
    
    /**
     * Returns an input stream for the given blob. The caller must close the stream when done.
     * 
     * @param path the blob file key for the file to be pulled
     * @return the blob's input stream
     */
    public InputStream pullStream(String path) throws BlobException;
    
    /**
     * Writes to the external store using an input stream for the given core. The unique
     * path to the written blob is returned.
     * 
     * @param blobName name of the core to be pulled from the store
     * @param is input stream of the core
     * @param contentLength size of the stream's data
     * @param fileNamePrefix have this string followed by a "." be how the "filename" of the written blob starts
     * @return the unique path to the written blob. Expected to be of the form /path1/.../pathn/_filenamePrefix_._random string_
     */
    public String pushStream(String blobName, InputStream is, long contentLength, String fileNamePrefix) throws BlobException;
    
    /**
     * Checks if the core metadata file exists for the given blobName.
     * 
     * @param blobName name of the core to check
     * @return true if the core has blobs
     */
    public boolean coreMetadataExists(String blobName) throws BlobException;
    
    /**
     * Deletes all blob files associated with this blobName.
     * 
     * @param blobName core to delete
     */
    public void deleteCore(String blobName) throws BlobException;
    
    /**
     * Batch delete blob files from the blob store. Any blob file path that specifies a non-existent blob file will 
     * not be treated as an error and should return success.
     * 
     * @param paths list of blob file keys to the files to be deleted
     */
    public void deleteBlobs(Collection<String> paths) throws BlobException;
    
    /**
     * Retrieves an identifier for the cloud service providing the blobstore
     * 
     * @return string identifying the service provider 
     */
    public BlobstoreProviderType getStorageProvider();
    
    /**
     * Returns an identifier for the geographical region a bucket is located in. Most blob storage providers utilize the concept of regions. In the unlikely case that we use a provider that 
     * doesn't, we should update this method (and log lines that mostly use this) to reflect a more generic terminology.
     * 
     * @return string identifying the bucket's geographical region
     */
    public String getBucketRegion();
    
    /**
     * Returns the name of the bucket that this client will interact with
     * 
     * @return string name of the bucket this client is configured with
     */
    public String getBucketName();
    
    /**
     * Returns the the end point this client is configured with
     * 
     * @return string name of the endpoint this client is configured with
     */
    public String getEndpoint();
    
    /**
     * Tests whether or not a bucket exists. Throws an exception if something is wrong or fa
     * 
     * @return true if it does
     */
    public boolean doesBucketExist() throws BlobException;
    
    /**
     * Closes any resources used by the core storage client
     */
    public void shutdown();
    
    /**
     * Lists the blob file names of all of files listed under a given core name's blob store
     * hierarchy that are older than the given timestamp value in milliseconds. Important to 
     * note that that the wall clock of your caller will vary with that of the blob store service
     * such that you'll likely see mismatch if you're trying to list very recent files.
     * 
     * This method is intended for observing significantly older modified files where clock skew
     * is less of an issue.
     * 
     * @param blobName the core to be listed
     * @param timestamp timestamp in milliseconds
     * @return the list of blob files
     */
    public List<String> listCoreBlobFilesOlderThan(String blobName, long timestamp) throws BlobException;
    
    /**
     * Lists the common delimiter-terminated prefixes by blob keys beginning with the
     * provided prefix string.
     * 
     * e.g. "core1/name1", "core1/name2" and "core2/name2"
     * Passing the prefix argument "core" should return
     * "core1/" and "core2/" i.e. the common delimiter-terminated
     * prefixes shared by blob keys in the blob store.
     * 
     * @param prefix of the blob key to list by
     * @return the list of common prefixes
     */
    public List<String> listCommonBlobPrefix(String prefix) throws BlobException;
}
