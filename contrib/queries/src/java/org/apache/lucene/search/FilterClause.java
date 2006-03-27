package org.apache.lucene.search;

import org.apache.lucene.search.BooleanClause.Occur;

/**
 * A Filter that wrapped with an indication of how that filter
 * is used when composed with another filter.
 * (Follows the boolean logic in BooleanClause for composition 
 * of queries.)
 * @author BPDThebault
 */

public class FilterClause implements java.io.Serializable
{
	Occur occur = null;
	Filter filter = null;

	/**
	 * Create a new FilterClause
	 * @param filter A Filter object containing a BitSet
	 * @param occur A parameter implementation indicating SHOULD, MUST or MUST NOT
	 */
	
	public FilterClause( Filter filter,Occur occur)
	{
		this.occur = occur;
		this.filter = filter;
	}

	/**
	 * Returns this FilterClause's filter
	 * @return A Filter object
	 */
	
	public Filter getFilter()
	{
		return filter;
	}

	/**
	 * Returns this FilterClause's occur parameter
	 * @return An Occur object
	 */
	
	public Occur getOccur()
	{
		return occur;
	}

}
