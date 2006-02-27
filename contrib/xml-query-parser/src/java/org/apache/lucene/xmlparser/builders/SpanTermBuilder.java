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
		String fieldName=DOMUtils.getAttributeWithInheritance(e,"fieldName");
		String value=DOMUtils.getText(e);
		if((fieldName==null)||(fieldName.length()==0))
		{
			throw new ParserException("SpanTermQuery missing fieldName property ");
		}
		if((value==null)||(value.length()==0))
		{
			throw new ParserException("TermQuery missing value property ");
		}
		SpanTermQuery stq = new SpanTermQuery(new Term(fieldName,value));
		
		stq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
		return stq;
	}

}
