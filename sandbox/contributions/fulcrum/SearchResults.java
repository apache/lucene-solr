import org.apache.log4j.Category;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Hits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import search.SearchResultFactory;

/**
 * <p>
 * Encapsulates the results of a search. After a SearchResults has
 * been constructed from a Hits object, the IndexSearcher can be
 * safely closed.
 * </p>
 * <p>
 * SearchResults also provides a way of retrieving Java objects from
 * Documents (via {@link search.SearchResultsFactory}).
 * </p>
 * <p>
 * <b>Note that this implementation uses code from
 * /projects/appex/search.</b>
 * </p>
 */
public class SearchResults
{
    private static Category cat = Category.getInstance(SearchResults.class);
    private List hitsDocuments;
    private List objectResults;
    private int totalNumberOfResults;

    public SearchResults(Hits hits) throws IOException
    {
        this(hits, 0, hits.length());
    }

    public SearchResults(Hits hits, int from, int to) throws IOException
    {
        hitsDocuments = new ArrayList();
        totalNumberOfResults = hits.length();
        if (to > totalNumberOfResults)
        {
            to = totalNumberOfResults;
        }
        for (int i = from; i < to; i++)
        {
            hitsDocuments.add(hits.doc(i));
        }
    }

    public int getTotalNumberOfResults()
    {
        return totalNumberOfResults;
    }

    /**
     * Obtain the results of the search as objects.
     */
    public List getResultsAsObjects()
    {
        if (objectResults == null)
        {
            objectResults = new ArrayList();
            for (Iterator it = hitsDocuments.iterator(); it.hasNext();)
            {
                try
                {
                    Object o = SearchResultFactory.
                            getDocAsObject((Document) it.next());
                    if (!objectResults.contains(o))
                        objectResults.add(o);
                }
                catch (Exception e)
                {
                    cat.error("Error instantiating an object from a document.", e);
                }
            }
        }
        return objectResults;
    }
}
