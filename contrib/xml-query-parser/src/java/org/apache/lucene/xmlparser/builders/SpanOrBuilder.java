package org.apache.lucene.xmlparser.builders;

import java.util.ArrayList;

import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class SpanOrBuilder extends SpanBuilderBase
{
    
    SpanQueryBuilder factory;
    
    public SpanOrBuilder(SpanQueryBuilder factory)
    {
        super();
        this.factory = factory;
    }
    
	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
	    ArrayList clausesList=new ArrayList();
		for (Node kid = e.getFirstChild(); kid != null; kid = kid.getNextSibling())
		{
			if (kid.getNodeType() == Node.ELEMENT_NODE) 
			{
				SpanQuery clause=factory.getSpanQuery((Element) kid);
				clausesList.add(clause);				
			}
		}	    
		SpanQuery[] clauses=(SpanQuery[]) clausesList.toArray(new SpanQuery[clausesList.size()]);
		SpanOrQuery soq = new SpanOrQuery(clauses);		
		soq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return soq;
	}

}
