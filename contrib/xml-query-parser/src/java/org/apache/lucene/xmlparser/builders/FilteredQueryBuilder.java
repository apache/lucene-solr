/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.FilterBuilder;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;


/**
 * @author maharwood
 */
public class FilteredQueryBuilder implements QueryBuilder {
	
	private FilterBuilder filterFactory;
	private QueryBuilder queryFactory;

	public FilteredQueryBuilder(FilterBuilder filterFactory, QueryBuilder queryFactory)
	{
		this.filterFactory=filterFactory;
		this.queryFactory=queryFactory;
		
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
	 */
	public Query getQuery(Element e) throws ParserException {	
 	    Element filterElement=DOMUtils.getChildByTagOrFail(e,"Filter");
 	    filterElement=DOMUtils.getFirstChildOrFail(filterElement);
 	    Filter f=filterFactory.getFilter(filterElement);
 
 	    Element queryElement=DOMUtils.getChildByTagOrFail(e,"Query");
 	    queryElement=DOMUtils.getFirstChildOrFail(queryElement);
 	    Query q=queryFactory.getQuery(queryElement);
 	    
 	    FilteredQuery fq = new FilteredQuery(q,f);
 	    fq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
 	    return fq;		
	}

}
