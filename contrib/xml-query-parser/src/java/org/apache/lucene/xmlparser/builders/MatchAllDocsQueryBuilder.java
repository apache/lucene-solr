package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;

public class MatchAllDocsQueryBuilder implements QueryBuilder
{
	public Query getQuery(Element e) throws ParserException
	{
		return new MatchAllDocsQuery();
	}
}
