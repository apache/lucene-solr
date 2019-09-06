package org.apache.solr.store.blob;

/**
 * Exception thrown when a core is not present locally because it was deleted due to a corruption and we haven't yet pulled
 * another version from Blob Store.
 *
 * @author iginzburg
 * @since 216/solr.6
 */
public class DeletedCorruptCoreException extends Exception {
    public DeletedCorruptCoreException(String message) {
        super(message);
    }
}
