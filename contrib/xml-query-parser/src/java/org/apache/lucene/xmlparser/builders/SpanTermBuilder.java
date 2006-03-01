package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;

public class SpanTermBuilder extends SpanBuilderBase
{

	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
 		String fieldName=DOMUtils.getAttributeWithInheritanceOrFail(e,"fieldName");
 		String value=DOMUtils.getNonBlankTextOrFail(e);
  		SpanTermQuery stq = new SpanTermQuery(new Term(fieldName,value));
  		
  		stq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return stq;		
		
	}

}
