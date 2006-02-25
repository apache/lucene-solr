/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser;

import java.util.HashMap;

import org.apache.lucene.search.Filter;
import org.w3c.dom.Element;

/**
 * @author maharwood
 */
public class FilterBuilderFactory implements FilterBuilder {

	HashMap builders=new HashMap();
	
	public Filter getFilter(Element n) throws ParserException {
		FilterBuilder builder=(FilterBuilder) builders.get(n.getNodeName());
		if(builder==null)
		{
			throw new ParserException("No FilterBuilder defined for node "+n.getNodeName()); 
		}
		return builder.getFilter(n); 
	}
	public void addBuilder(String nodeName,FilterBuilder builder)
	{
		builders.put(nodeName,builder);
	}
	
}
