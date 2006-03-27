/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.FilterBuilder;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


/**
 * @author maharwood 
 */
public class BooleanFilterBuilder implements FilterBuilder {
	
	private FilterBuilder factory;

	public BooleanFilterBuilder(FilterBuilder factory)
	{
		this.factory=factory;
	}

	public Filter getFilter(Element e) throws ParserException {
		BooleanFilter bf=new BooleanFilter();
		NodeList nl = e.getElementsByTagName("Clause");
		for(int i=0;i<nl.getLength();i++)
		{
			Element clauseElem=(Element) nl.item(i);
			BooleanClause.Occur occurs=BooleanQueryBuilder.getOccursValue(clauseElem);
			
 			Element clauseFilter=DOMUtils.getFirstChildOrFail(clauseElem);
 			Filter f=factory.getFilter(clauseFilter);
 			bf.add(new FilterClause(f,occurs));			
		}
		
		return bf;
	}

}
