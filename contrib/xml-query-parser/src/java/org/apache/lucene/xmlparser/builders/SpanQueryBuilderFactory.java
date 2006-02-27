package org.apache.lucene.xmlparser.builders;

import java.util.HashMap;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;

/**
 * @author maharwood
 */
public class SpanQueryBuilderFactory implements SpanQueryBuilder {

	HashMap builders=new HashMap();
	
	public Query getQuery(Element e) throws ParserException {
		return getSpanQuery(e);
	}
	public void addBuilder(String nodeName,SpanQueryBuilder builder)
	{
		builders.put(nodeName,builder);
	}
	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
		SpanQueryBuilder builder=(SpanQueryBuilder) builders.get(e.getNodeName());
		if(builder==null)
		{
			throw new ParserException("No SpanQueryObjectBuilder defined for node "+e.getNodeName()); 
		}
		return builder.getSpanQuery(e); 
	}

}
