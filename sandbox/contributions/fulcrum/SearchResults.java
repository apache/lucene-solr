import org.apache.log4j.Category;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Hits;
import search.SearchResultFactory;

import java.io.IOException;

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
    private Document[] hitsDocuments;
    private Object[] objectResults;
    private int totalNumberOfResults;

    public SearchResults(Hits hits) throws IOException
    {
        this(hits, 0, hits.length() - 1);
    }

    public SearchResults(Hits hits, int from, int to) throws IOException
    {
        totalNumberOfResults = hits.length();
        if (to > totalNumberOfResults)
        {
            to = totalNumberOfResults - 1;
        }
        int numberOfResults = to - from + 1;
        if (numberOfResults > -1)
        {
            hitsDocuments = new Document[numberOfResults];
            for (int i = to, j = 0; i >= from; i--, j++)
            {
                hitsDocuments[j] = hits.doc(i);
            }
        }
        else
        {
            throw new IllegalArgumentException("Range of results requested " +
                                               "exceed total number of search " +
                                               "results returned.");
        }
    }

    public int getTotalNumberOfResults()
    {
        return totalNumberOfResults;
    }

    /**
     * Obtain the results of the search as objects. The objects returned are
     * not guaranteed to be unique.
     */
    public Object[] getResultsAsObjects()
    {
        if (objectResults == null)
        {
            objectResults = new Object[hitsDocuments.length];
            for (int i = 0; i < hitsDocuments.length; i++)
            {
                try
                {
                    objectResults[i] = SearchResultFactory.
                            getDocAsObject(hitsDocuments[i]);
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
