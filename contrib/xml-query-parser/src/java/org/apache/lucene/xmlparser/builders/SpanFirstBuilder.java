package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;

public class SpanFirstBuilder extends SpanBuilderBase
{
    SpanQueryBuilder factory;
    
    public SpanFirstBuilder(SpanQueryBuilder factory)
    {
        super();
        this.factory = factory;
    }

	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
	    int end=DOMUtils.getAttribute(e,"end",1);
	    Element child=DOMUtils.getFirstChildElement(e);
	    SpanQuery q=factory.getSpanQuery(child);
	    
		SpanFirstQuery sfq = new SpanFirstQuery(q,end);
		
		sfq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return sfq;
	}

}
