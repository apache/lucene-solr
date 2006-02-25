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
		
		Element mainQueryElem=DOMUtils.getChildByTagName(e,"Query");
		if(mainQueryElem==null)
		{
			throw new ParserException("BoostingQuery missing a \"Query\" child element");
		}
		mainQueryElem=DOMUtils.getFirstChildElement(mainQueryElem);
		if(mainQueryElem==null)
		{
			throw new ParserException("BoostingQuery \"Query\" element missing a child element");
		}
		Query mainQuery=factory.getQuery(mainQueryElem);
		

		Element boostQueryElem=DOMUtils.getChildByTagName(e,"BoostQuery");
		float boost=DOMUtils.getAttribute(boostQueryElem,"boost",defaultBoost);
		if(boostQueryElem==null)
		{
			throw new ParserException("BoostingQuery missing a \"BoostQuery\" child element");
		}
		boostQueryElem=DOMUtils.getFirstChildElement(boostQueryElem);
		if(boostQueryElem==null)
		{
			throw new ParserException("BoostingQuery \"BoostQuery\" element missing a child element");
		}
		Query boostQuery=factory.getQuery(boostQueryElem);
		
		BoostingQuery bq = new BoostingQuery(mainQuery,boostQuery,boost);
		bq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return bq;

	}


}
