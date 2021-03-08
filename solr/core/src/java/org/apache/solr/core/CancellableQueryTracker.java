package org.apache.solr.core;

import org.apache.lucene.search.CancellableCollector;
import org.apache.lucene.search.CancellableTask;
import org.apache.solr.request.SolrQueryRequest;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.solr.common.params.CommonParams.QUERY_UUID;

/**
 * Tracks metadata for active queries and provides methods for access
 */
public class CancellableQueryTracker {
    //TODO: This needs to become a time aware storage model
    private final Map<String, CancellableTask> activeCancellableQueries = new ConcurrentHashMap<>();
    private final Map<String, String> activeQueriesGenerated = new ConcurrentHashMap<>();

    /** Generates a UUID for the given query or if the user provided a UUID
     * for this query, uses that.
     */
    public String generateQueryID(SolrQueryRequest req) {
        String queryID;
        String customQueryUUID = req.getParams().get(QUERY_UUID, null);

        if (customQueryUUID != null) {
            queryID = customQueryUUID;
        } else {
            queryID = UUID.randomUUID().toString();
        }

        if (activeQueriesGenerated.containsKey(queryID)) {
            if (customQueryUUID != null) {
                throw new IllegalArgumentException("Duplicate query UUID given");
            } else {
                while (activeQueriesGenerated.get(queryID) != null) {
                    queryID = UUID.randomUUID().toString();
                }
            }
        }

        activeQueriesGenerated.put(queryID, req.getHttpSolrCall().getReq().getQueryString());

        return queryID;
    }

    public void releaseQueryID(String inputQueryID) {
        if (inputQueryID == null) {
            return;
        }

        activeQueriesGenerated.remove(inputQueryID);
    }

    public boolean isQueryIdActive(String queryID) {
        return activeQueriesGenerated.containsKey(queryID);
    }

    public void addShardLevelActiveQuery(String queryID, CancellableCollector collector) {
        if (queryID == null) {
            return;
        }

        activeCancellableQueries.put(queryID, collector);
    }

    public CancellableTask getCancellableTask(String queryID) {
        if (queryID == null) {
            throw new IllegalArgumentException("Input queryID is null");
        }

        return activeCancellableQueries.get(queryID);
    }

    public void removeCancellableQuery(String queryID) {
        if (queryID == null) {
            // Some components, such as CaffeineCache, use the searcher to fire internal queries which are not tracked
            return;
        }

        activeCancellableQueries.remove(queryID);
    }

    public Iterator<Map.Entry<String, String>> getActiveQueriesGenerated() {
        return activeQueriesGenerated.entrySet().iterator();
    }
}
