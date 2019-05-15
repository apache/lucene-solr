package org.apache.solr.store.blob.client;

/**
 * 
 * Extension of BlobException that represents an error on the client-side when attempting to make
 * a request to the underlying blob store service. An exception of this type indicates that the
 * client was unable to successfully make the service call.  
 *
 * @author a.vuong
 * @since 218/solr.6
 */
public class BlobClientException extends BlobException {
    
    public BlobClientException(Throwable cause) {
        super(cause);
    }

    public BlobClientException(String message) {
        super(message);
    }

    public BlobClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
