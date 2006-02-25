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
	    Element includeElem=DOMUtils.getChildByTagName(e,"Include");
	    if(includeElem!=null)
		{
	        includeElem=DOMUtils.getFirstChildElement(includeElem);
		}
	    if(includeElem==null)
	    {
			throw new ParserException("SpanNotQuery missing Include child Element");	        
	    }
	    Element excludeElem=DOMUtils.getChildByTagName(e,"Exclude");
	    if(excludeElem!=null)
		{
	        excludeElem=DOMUtils.getFirstChildElement(excludeElem);
		}
	    if(excludeElem==null)
	    {
			throw new ParserException("SpanNotQuery missing Exclude child Element");	        
	    }
	    SpanQuery include=factory.getSpanQuery(includeElem);
	    SpanQuery exclude=factory.getSpanQuery(excludeElem);
	    
		SpanNotQuery snq = new SpanNotQuery(include,exclude);
		
		snq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return snq;
	}

}
