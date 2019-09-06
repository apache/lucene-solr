package org.apache.solr.store.blob.client;

/**
 * Parent class of blob store related issues. Likely to change and maybe disappear but good enough for a PoC.
 *
 * @author iginzburg
 * @since 214/solr.6
 */
public class BlobException extends Exception {
    public BlobException(Throwable cause) {
        super(cause);
    }

    public BlobException(String message) {
        super(message);
    }

    public BlobException(String message, Throwable cause) {
        super(message, cause);
    }
}
