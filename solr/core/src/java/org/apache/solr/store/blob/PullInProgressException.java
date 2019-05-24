package org.apache.solr.store.blob.util;

/**
 * Exception thrown when a new pull of a core cannot be initiated because a pull is in progress but the core is not yet
 * available so can't be used and request can't be handled.
 *
 * @author iginzburg
 * @since 216/solr.6
 */
public class PullInProgressException extends Exception {
    public PullInProgressException(String message) {
        super(message);
    }
}
