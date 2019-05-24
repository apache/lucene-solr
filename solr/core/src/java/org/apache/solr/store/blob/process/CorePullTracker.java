package org.apache.solr.store.blob.process;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

import javax.servlet.http.HttpServletRequest;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrRequestParsers;
import org.eclipse.jetty.http.HttpMethods;

import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.CheckForNull;

import searchserver.SfdcConfig;
import searchserver.SfdcConfigProperty;
import searchserver.blobstore.process.CorePullerFeeder.PullCoreInfo;
import searchserver.blobstore.util.DeduplicatingList;
import searchserver.core.CoreEpoch;
import searchserver.core.SfdcCoreName;
import searchserver.handler.SynonymDataHandler;
import searchserver.logging.SearchLogger;
import searchserver.update.processor.SfdcMetadataProcessorFactory;


/**
 * Tracks cores that are being queried and if necessary enqueues them for pull from blob store
 *
 * @author msiddavanahalli
 * @since 214/solr.6
 */
public class CorePullTracker {
    private static final SearchLogger logger = new SearchLogger(CorePullTracker.class);

    static private final int TRACKING_LIST_MAX_SIZE = 500000;

    private final DeduplicatingList<String, PullCoreInfo> coresToPull;

    /* Config value that enables core pulls */
    @VisibleForTesting
    public static boolean isBackgroundPullEnabled = Boolean
            .parseBoolean(SfdcConfig.get().getSfdcConfigProperty(SfdcConfigProperty.EnableBlobBackgroundPulling));

    private static CorePullTracker INSTANCE = null;

    // Let's define these paths in yet another place in the code...
    private static final String QUERY_PATH_PREFIX = "/select";
    private static final String SPELLCHECK_PATH_PREFIX = "/spellcheck";
    private static final String RESULTPROMOTION_PATH_PREFIX = "/result_promotion";
    private static final String INDEXLOOKUP_PATH_PREFIX = "/indexLookup";
    private static final String HIGHLIGHT_PATH_PREFIX = "/highlight";
    private static final String BACKUP_PATH_PREFIX = "/backup";

    /**
     * Get the CorePullTracker instance to track cores that need to be pulled in from Blob.
     */
    public synchronized static CorePullTracker get() {
        if (INSTANCE == null) {
            INSTANCE = new CorePullTracker();
        }
        return INSTANCE;
    }

    public CorePullTracker() {
        coresToPull = new DeduplicatingList<>(TRACKING_LIST_MAX_SIZE, new CorePullerFeeder.PullCoreInfoMerger());
    }

    /**
     * If the local core is stale, enqueues it to be pulled in from blob Note : If there is no coreName available in the
     * requestPath we simply ignore the request.
     */
    public void enqueueForPullIfNecessary(HttpServletRequest request, CoreContainer cores) throws IOException {
        String servletPath = request.getServletPath();

        String coreName = SfdcCoreName.getCorenameFromRequestPath(servletPath);
        Map<String, String[]> params = getRequestParams(request);
        // TODO: do we need isBackgroundPullEnabled in addition to isBlobEnabled? If not we should remove this.
        if (isBackgroundPullEnabled && coreName != null && shouldPullStale(servletPath)
                && !isLocalCoreUpToDate(params, coreName, cores)) {
            enqueueForPull(coreName, false, false);
        }
    }

    /**
     * Enqueues a core for pull
     * @param coreName name of the core
     * @param createCoreIfAbsent whether to create core before pulling if absent
     * @param waitForSearcher whether to wait for newly pulled contents be reflected through searcher 
     */
    public void enqueueForPull(String coreName, boolean createCoreIfAbsent, boolean waitForSearcher) {
        PullCoreInfo pci = new PullCoreInfo(coreName, waitForSearcher, createCoreIfAbsent);
        try {
            coresToPull.addDeduplicated(pci, false);
        } catch (InterruptedException ie) {
            logger.log(Level.WARNING, null, "Core " + coreName + " not added to Blob pull list. System shutting down?");
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
        if (HttpMethods.POST.equals(method)) {
            params = SolrRequestParsers.parseQueryString(request.getQueryString()).getMap();
        } else {
            params = request.getParameterMap();
        }

        return params;
    }

    /**
     * Determines if our local core is up to date
     */
    private boolean isLocalCoreUpToDate(Map<String, String[]> params, @CheckForNull String coreName,
                                        CoreContainer cores) throws IOException {

        CoreEpoch localCoreEpoch = new CoreEpoch(coreName, cores);
        // For local core freshness checks, we should compare our local replay count against what the app expects
        // our last acknowledged sequence number (replay) to be (see W-2864356).
        String requestedCoreReplay = getParameterValue(params, SfdcMetadataProcessorFactory.LAST_ACKNOWLEDGED, null);
        String requestedCoreGeneration = getParameterValue(params, SfdcMetadataProcessorFactory.GENERATION, null);
        CoreEpoch requestedCoreEpoch = (Strings.isNullOrEmpty(requestedCoreReplay)
                || Strings.isNullOrEmpty(requestedCoreGeneration)) ? new CoreEpoch(0, 0)
                : new CoreEpoch(requestedCoreReplay, requestedCoreGeneration);

        return localCoreEpoch.isFreshEnough(requestedCoreEpoch);
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
     * Determine if our request should trigger a pull from Blob when local core is stale.<p>
     * /update path is handled in {@link searchserver.update.processor.SfdcMetadataProcessor}
     */
    private boolean shouldPullStale(String servletPath) {
        // get the request handler from the path (taken from SolrDispatchFilter)
        int idx = servletPath.indexOf('/', 1);
        if (idx > 1) {
            String action = servletPath.substring(idx);
            // Note: if new paths are added for new types of access to the searchserver, the set of paths that trigger
            // a pull of a stale core below might have to be updated.
            return action.startsWith(QUERY_PATH_PREFIX)
                    || action.startsWith(SPELLCHECK_PATH_PREFIX)
                    || action.startsWith(SynonymDataHandler.SYNONYM_DATA_HANDLER_PATH)
                    || action.startsWith(RESULTPROMOTION_PATH_PREFIX)
                    || action.startsWith(INDEXLOOKUP_PATH_PREFIX)
                    || action.startsWith(HIGHLIGHT_PATH_PREFIX)
                    || action.startsWith(BACKUP_PATH_PREFIX);
        } else {
            return false;
        }
    }
}
