/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser;

import java.util.HashMap;

import org.apache.lucene.search.Query;
import org.w3c.dom.Element;

/**
 * @author maharwood
 */
public class QueryBuilderFactory implements QueryBuilder {

	HashMap builders=new HashMap();
	
	public Query getQuery(Element n) throws ParserException {
		QueryBuilder builder=(QueryBuilder) builders.get(n.getNodeName());
		if(builder==null)
		{
			throw new ParserException("No QueryObjectBuilder defined for node "+n.getNodeName()); 
		}
		return builder.getQuery(n); 
	}
	public void addBuilder(String nodeName,QueryBuilder builder)
	{
		builders.put(nodeName,builder);
	}

}
