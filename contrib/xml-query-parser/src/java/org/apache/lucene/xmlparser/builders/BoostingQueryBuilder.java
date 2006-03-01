package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.BoostingQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;


public class BoostingQueryBuilder implements QueryBuilder
{
	
	private QueryBuilder factory;
	float defaultBoost=0.01f;

	public BoostingQueryBuilder (QueryBuilder factory)
	{
		this.factory=factory;
	}

	public Query getQuery(Element e) throws ParserException
	{
		
        Element mainQueryElem=DOMUtils.getChildByTagOrFail(e,"Query");
 		mainQueryElem=DOMUtils.getFirstChildOrFail(mainQueryElem);
  		Query mainQuery=factory.getQuery(mainQueryElem);

 		Element boostQueryElem=DOMUtils.getChildByTagOrFail(e,"BoostQuery");
  		float boost=DOMUtils.getAttribute(boostQueryElem,"boost",defaultBoost);
 		boostQueryElem=DOMUtils.getFirstChildOrFail(boostQueryElem);
  		Query boostQuery=factory.getQuery(boostQueryElem);
  		
  		BoostingQuery bq = new BoostingQuery(mainQuery,boostQuery,boost);

  		bq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return bq;

	}


}
