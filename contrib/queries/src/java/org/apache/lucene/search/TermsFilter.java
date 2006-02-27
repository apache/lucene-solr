package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

/**
 * Constructs a filter for docs matching any of the terms added to this class. 
 * Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in 
 * a sequence. An example might be a collection of primary keys from a database query result or perhaps 
 * a choice of "category" labels picked by the end user. As a filter, this is much faster than the 
 * equivalent query (a BooleanQuery with many "should" TermQueries)
 * 
 * @author maharwood
 */
public class TermsFilter extends Filter
{
	ArrayList termsList=new ArrayList();
	
	/**
	 * Adds a term to the list of acceptable terms   
	 * @param term
	 */
	public void addTerm(Term term)
	{
		termsList.add(term);
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.search.Filter#bits(org.apache.lucene.index.IndexReader)
	 */
	public BitSet bits(IndexReader reader) throws IOException
	{
		BitSet result=new BitSet(reader.maxDoc());
		for (Iterator iter = termsList.iterator(); iter.hasNext();)
		{
			Term term = (Term) iter.next();
			TermDocs td=reader.termDocs(term);
	        while (td.next())
	        {
	            result.set(td.doc());
	        }						
		}
		return result;
	}
}
