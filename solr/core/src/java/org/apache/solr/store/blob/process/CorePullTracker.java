package org.apache.solr.store.blob.process;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.store.blob.metadata.PushPullData;
import org.apache.solr.store.blob.process.CorePullerFeeder.PullCoreInfo;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.apache.solr.store.blob.util.DeduplicatingList;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks cores that are being queried and if necessary enqueues them for pull from blob store
 *
 * @author msiddavanahalli
 * @since 214/solr.6
 */
public class CorePullTracker {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static private final int TRACKING_LIST_MAX_SIZE = 500000;

  private final DeduplicatingList<String, PullCoreInfo> coresToPull;

  /* Config value that enables core pulls */
  @VisibleForTesting
  public static boolean isBackgroundPullEnabled = true; // TODO : make configurable

  // Let's define these paths in yet another place in the code...
  private static final String QUERY_PATH_PREFIX = "/select";
  private static final String SPELLCHECK_PATH_PREFIX = "/spellcheck";
  private static final String RESULTPROMOTION_PATH_PREFIX = "/result_promotion";
  private static final String INDEXLOOKUP_PATH_PREFIX = "/indexLookup";
  private static final String HIGHLIGHT_PATH_PREFIX = "/highlight";
  private static final String BACKUP_PATH_PREFIX = "/backup";

  public CorePullTracker() {
    coresToPull = new DeduplicatingList<>(TRACKING_LIST_MAX_SIZE, new CorePullerFeeder.PullCoreInfoMerger());
  }

  /**
   * If the local core is stale, enqueues it to be pulled in from blob
   * TODO: add stricter checks so that we don't pull on every request
   */
  public void enqueueForPullIfNecessary(String requestPath, SolrCore core, String collectionName,
      CoreContainer cores) throws IOException, SolrException {
    // Initialize variables
    String coreName = core.getName();
    String shardName = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    SharedShardMetadataController sharedShardMetadataController = cores.getSharedStoreManager().getSharedShardMetadataController();
    DocCollection collection = cores.getZkController().getClusterState().getCollection(collectionName);

    Slice shard = collection.getSlicesMap().get(shardName);
    if (shard != null) {
      try {
        if (!collection.getActiveSlices().contains(shard)) {
          // unclear if there are side effects but logging for now
          logger.warn("Enqueueing a pull for shard " + shardName + " that is inactive!");
        }
        logger.info("Enqueue a pull for collection=" + collectionName + " shard=" + shardName + " coreName=" + coreName);
        // creates the metadata node if it doesn't exist
        sharedShardMetadataController.ensureMetadataNodeExists(collectionName, shardName);

        /*
         * Get the metadataSuffix value from ZooKeeper or from a cache if an entry exists for the 
         * given collection and shardName. If the leader has already changed, the conditional update
         * later will fail and invalidate the cache entry if it exists. 
         */
        VersionedData data = sharedShardMetadataController.readMetadataValue(collectionName, shardName, 
            /* readFromCache */ true);
        Map<String, String> nodeUserData = (Map<String, String>) Utils.fromJSON(data.getData());
        String metadataSuffix = nodeUserData.get(SharedShardMetadataController.SUFFIX_NODE_NAME);

        String sharedShardName = (String) shard.get(ZkStateReader.SHARED_SHARD_NAME);

        PushPullData pushPullData = new PushPullData.Builder()
            .setCollectionName(collectionName)
            .setShardName(shardName)
            .setCoreName(coreName)
            .setSharedStoreName(sharedShardName)
            .setLastReadMetadataSuffix(metadataSuffix)
            .setNewMetadataSuffix(BlobStoreUtils.generateMetadataSuffix())
            .setZkVersion(data.getVersion())
            .build();

        enqueueForPullIfNecessary(requestPath, pushPullData, cores);

      } catch (Exception ex) {
        // wrap every thrown exception in a solr exception
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error trying to push to blob store", ex);
      }
    }
  }

  /**
   * If the local core is stale, enqueues it to be pulled in from blob Note : If there is no coreName available in the
   * requestPath we simply ignore the request.
   */
  public void enqueueForPullIfNecessary(String requestPath, PushPullData pushPullData, 
      CoreContainer cores) throws IOException {
    // TODO: do we need isBackgroundPullEnabled in addition to isBlobEnabled? If not we should remove this.
    // TODO: always pull for this hack - want to check if local core is up to date
    if (isBackgroundPullEnabled && pushPullData.getSharedStoreName() != null && shouldPullStale(requestPath)) {
      logger.info("Enqueuing pull on path " + requestPath);
      enqueueForPull(pushPullData, false, false);
    }
  }

  /**
   * Enqueues a core for pull
   * 
   * @param pushPullData pull request data required to interact with blob store
   * @param createCoreIfAbsent whether to create core before pulling if absent
   * @param waitForSearcher whether to wait for newly pulled contents be reflected through searcher 
   */
  public void enqueueForPull(PushPullData pushPullData, boolean createCoreIfAbsent, boolean waitForSearcher) {
    PullCoreInfo pci = new PullCoreInfo(pushPullData, waitForSearcher, createCoreIfAbsent);
    try {
      coresToPull.addDeduplicated(pci, false);
    } catch (InterruptedException ie) {
      logger.warn("Core " + pushPullData.getSharedStoreName() + " not added to Blob pull list. System shutting down?");
      Thread.currentThread().interrupt();
    }
  }

  public PullCoreInfo getCoreToPull() throws InterruptedException {
    return coresToPull.removeFirst();
  }

  /**
   * Get a set of request params for the given request
   */
  private Map<String, String[]> getRequestParams(HttpServletRequest request) {
    Map<String, String[]> params;

    // if this is a POST, calling request#getParameter(String) may cause the input stream (body) to be read, in
    // search of that parameter. Any subsequent calls to read the request body will fail, as the stream is then
    // empty.
    String method = request.getMethod();
    if ("POST".equals(method)) {
      params = SolrRequestParsers.parseQueryString(request.getQueryString()).getMap();
    } else {
      params = request.getParameterMap();
    }

    return params;
  }

  /** @return the *first* value for the specified parameter or {@code defaultValue}, if not present. */
  private String getParameterValue(Map<String, String[]> params, String parameterName, String defaultValue) {
    String[] values = params.get(parameterName);
    if (values != null && values.length == 1) {
      return values[0];
    } else {
      return defaultValue;
    }
  }

  /**
   * Determine if our request should trigger a pull from Blob when local core is stale.
   */
  private boolean shouldPullStale(String servletPath) {
    // get the request handler from the path (taken from SolrDispatchFilter)
    int idx = servletPath.indexOf('/');
    if (idx != -1) {
      String action = servletPath.substring(idx);
      return action.startsWith(QUERY_PATH_PREFIX)
          || action.startsWith(SPELLCHECK_PATH_PREFIX)
          // TODO || action.startsWith(SynonymDataHandler.SYNONYM_DATA_HANDLER_PATH)
          || action.startsWith(RESULTPROMOTION_PATH_PREFIX)
          || action.startsWith(INDEXLOOKUP_PATH_PREFIX)
          || action.startsWith(HIGHLIGHT_PATH_PREFIX)
          || action.startsWith(BACKUP_PATH_PREFIX);
    } else {
      logger.warn("Not pulling for specified path " + servletPath);
      return false;
    }
  }
}
