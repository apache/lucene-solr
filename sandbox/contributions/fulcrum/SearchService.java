import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.document.Document;
import org.apache.fulcrum.ServiceException;
import org.apache.fulcrum.Service;

import java.util.Map;

/**
 * A SearchService based on the Fulcrum services framework.
 */
public interface SearchService extends Service
{
    /**
     * The key in the TurbineResources.properties that references this
     * service.
     */
    public static final String SERVICE_NAME = "SearchService";

    /**
     * The key in SearchService properties in
     * TurbineResources.properties. The location of the index.
     * Assumes a FSDirectory is used.
     */
    public static final String INDEX_LOCATION_KEY = "index.location";

    /**
     * Performs a search.
     *
     * @param Query to search on.
     * @return SearchResults
     * @exception ServiceException
     */
    public SearchResults search(Query query) throws ServiceException;

    /**
     * Performs a search, using a filter to filter the results.
     *
     * @param Query to search on.
     * @param Filter to filter the results through.
     * @return SearchResults
     * @exception ServiceException
     */
    public SearchResults search(Query query, Filter filter) throws ServiceException;

    /**
     * Performs a search, using a filter to filter the results, then
     * return the results within the range specified.
     *
     * @param Query to search on.
     * @return SearchResults
     * @exception ServiceException
     */
    public SearchResults search(Query query, Filter filter,
                                int from, int to) throws ServiceException;

    /**
     * Refresh the entire index.
     */
    public void batchIndex() throws ServiceException;

    /**
     * Is the indexer currently indexing?
     */
    public boolean isIndexing();

    /**
     * Get the analyzer used.
     */
    public Analyzer getAnalyzer();
}
