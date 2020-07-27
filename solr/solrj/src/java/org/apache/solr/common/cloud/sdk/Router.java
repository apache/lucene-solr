package org.apache.solr.common.cloud.sdk;

/**Route documents to appropriate shard */
public interface Router {
    /**shard name for a given routing key
     *
     */
    String shard(String routingKey);
}
