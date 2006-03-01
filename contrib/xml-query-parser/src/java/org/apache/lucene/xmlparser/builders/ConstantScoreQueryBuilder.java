package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.FilterBuilderFactory;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;

public class ConstantScoreQueryBuilder implements QueryBuilder
{
	private FilterBuilderFactory filterFactory;

	public ConstantScoreQueryBuilder(FilterBuilderFactory filterFactory)
	{
		this.filterFactory=filterFactory;
	}

	public Query getQuery(Element e) throws ParserException
	{
 		Element filterElem=DOMUtils.getFirstChildOrFail(e);
  		Query q=new ConstantScoreQuery(filterFactory.getFilter(filterElem));

  		q.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));

  		return q;
		
	}

}
