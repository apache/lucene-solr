package org.apache.solr.search.sorting;

import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.List;

/**
 * Interface to be implemented by Solr collectors in order to identify those collectors that are aware that some of the docs that they collect are
 * equivalent to each other and thus need additional tie breaking ranking associated with them. Docs that are considered equivalent when ranking, are
 * grouped together and are exposed via the {@link #getTieBreakerGroups()} method.
 */
public interface TBGAwareCollector extends Collector {
    TBGAwareCollector create(int numHits, boolean fillFields, boolean trackDocScores, boolean trackMaxScore);
    /**
     * Returns the list of TieBreakerGroups found within the page of results after collection completes. Each group is complete - it contains
     * all of the docs that have been collected that are considered equal for ranking purposes regardless if pagination would truncate the list.
     * This method must be called AFTER top docs have been generated.
     *
     * @return the list of TieBreakerGroups found within the page of results after collection completes
     */
    List<TieBreakerGroup> getTieBreakerGroups();

    TopDocs topDocs(int start);

    TopDocs topDocs(int start, int howMany);
    
    public boolean isSort();

    
    
}
