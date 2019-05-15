package org.apache.solr.store.blob.client;

/**
 * Enum of identifiers for the underlying blob store service 
 * 
 * @author a.vuong
 * @since 218
 */
public enum BlobstoreProviderType {
    /** Host's local disk */
    LOCAL_FILE_SYSTEM,
    /** Amazon Web Services - S3 */
    S3,
    /** Google Cloud Storage - GCS */
    GCS
}