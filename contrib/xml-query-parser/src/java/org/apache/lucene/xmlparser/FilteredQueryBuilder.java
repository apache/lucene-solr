/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
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
		Element filterElement=DOMUtils.getChildByTagName(e,"Filter");
		if(filterElement==null)
		{
			throw new ParserException("FilteredQuery missing \"Filter\" child element");
		}
		filterElement=DOMUtils.getFirstChildElement(filterElement);
		Filter f=null;
		if(filterElement!=null)
		{
			f=filterFactory.getFilter(filterElement);
		}
		else
		{
			throw new ParserException("FilteredQuery \"Filter\" element missing child query element ");
		}
		
		
		Element queryElement=DOMUtils.getChildByTagName(e,"Query");
		if(queryElement==null)
		{
			throw new ParserException("FilteredQuery missing \"Query\" child element");
		}
		queryElement=DOMUtils.getFirstChildElement(queryElement);
		Query q=null;
		if(queryElement!=null)
		{
			q=queryFactory.getQuery(queryElement);
		}
		else
		{
			throw new ParserException("FilteredQuery \"Query\" element missing child query element ");
		}

		
		FilteredQuery fq = new FilteredQuery(q,f);
		fq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return fq;

	}

}
