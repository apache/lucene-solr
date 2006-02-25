/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;

/**
 * @author maharwood
 */
public interface SpanQueryBuilder extends QueryBuilder{
	
	public SpanQuery getSpanQuery(Element e) throws ParserException;

}
