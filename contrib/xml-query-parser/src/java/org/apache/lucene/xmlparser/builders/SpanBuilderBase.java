package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;

public abstract class SpanBuilderBase implements SpanQueryBuilder
{
	public Query getQuery(Element e) throws ParserException
	{
		return getSpanQuery(e);
	}

}
