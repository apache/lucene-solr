package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;

public class SpanNotBuilder extends SpanBuilderBase
{
    
    SpanQueryBuilder factory;    

    /**
     * @param factory
     */
    public SpanNotBuilder(SpanQueryBuilder factory)
    {
        super();
        this.factory = factory;
    }
	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
  	    Element includeElem=DOMUtils.getChildByTagOrFail(e,"Include");
        includeElem=DOMUtils.getFirstChildOrFail(includeElem);

  	    Element excludeElem=DOMUtils.getChildByTagOrFail(e,"Exclude");
        excludeElem=DOMUtils.getFirstChildOrFail(excludeElem);

  	    SpanQuery include=factory.getSpanQuery(includeElem);
  	    SpanQuery exclude=factory.getSpanQuery(excludeElem);
	    
		SpanNotQuery snq = new SpanNotQuery(include,exclude);
		
		snq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return snq;
	}

}
