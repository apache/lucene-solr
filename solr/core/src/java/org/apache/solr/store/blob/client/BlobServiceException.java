package org.apache.solr.store.blob.client;

/**
 * 
 * Extension of BlobException that represents a response returned by the underlying blob store service.
 * Receiving an exception of this type indicates that the caller's request was successfully sent to 
 * the blob store but the service was unable to fulfill the request. 
 *
 * @author a.vuong
 * @since 218/solr.6
 */
public class BlobServiceException extends BlobException {
    
    public BlobServiceException(Throwable cause) {
        super(cause);
    }

    public BlobServiceException(String message) {
        super(message);
    }

    public BlobServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
