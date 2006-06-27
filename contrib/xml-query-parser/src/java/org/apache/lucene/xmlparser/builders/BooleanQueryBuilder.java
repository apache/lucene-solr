/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


/**
 * @author maharwood 
 */
public class BooleanQueryBuilder implements QueryBuilder {
	
	private QueryBuilder factory;

	public BooleanQueryBuilder(QueryBuilder factory)
	{
		this.factory=factory;
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
	 */
	public Query getQuery(Element e) throws ParserException {
		BooleanQuery bq=new BooleanQuery(DOMUtils.getAttribute(e,"disableCoord",false));
		bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(e,"minimumNumberShouldMatch",0));
		bq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		NodeList nl = e.getElementsByTagName("Clause");
		for(int i=0;i<nl.getLength();i++)
		{
			Element clauseElem=(Element) nl.item(i);
			BooleanClause.Occur occurs=getOccursValue(clauseElem);
			
 			Element clauseQuery=DOMUtils.getFirstChildOrFail(clauseElem);
 			Query q=factory.getQuery(clauseQuery);
 			bq.add(new BooleanClause(q,occurs));			
		}
		
		return bq;
	}
	static BooleanClause.Occur getOccursValue(Element clauseElem) throws ParserException
	{
		String occs=clauseElem.getAttribute("occurs");
		BooleanClause.Occur occurs=BooleanClause.Occur.SHOULD;
		if("must".equalsIgnoreCase(occs))
		{
			occurs=BooleanClause.Occur.MUST;
		}
		else
		{
			if("mustNot".equalsIgnoreCase(occs))
			{
				occurs=BooleanClause.Occur.MUST_NOT;
			}			
			else
			{
				if(("should".equalsIgnoreCase(occs))||("".equals(occs)))
				{
					occurs=BooleanClause.Occur.SHOULD;
				}			
				else				
				{
					if(occs!=null)
					{
						throw new ParserException("Invalid value for \"occurs\" attribute of clause:"+occs);
					}
				}
			}
		}
		return occurs;
		
	}

}
