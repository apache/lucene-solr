package org.apache.lucene.xmlparser.builders;

import java.util.ArrayList;

import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class SpanNearBuilder extends SpanBuilderBase
{
	SpanQueryBuilder factory;
	public SpanNearBuilder(SpanQueryBuilder factory)
	{
		this.factory=factory;
	}
	
	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
 		String slopString=DOMUtils.getAttributeOrFail(e,"slop");
  		int slop=Integer.parseInt(slopString);
		boolean inOrder=DOMUtils.getAttribute(e,"inOrder",false);
		ArrayList spans=new ArrayList();
		for (Node kid = e.getFirstChild(); kid != null; kid = kid.getNextSibling())
		{
				if (kid.getNodeType() == Node.ELEMENT_NODE) 
				{
					spans.add(factory.getSpanQuery((Element) kid));
				}
		}
		SpanQuery[] spanQueries=(SpanQuery[]) spans.toArray(new SpanQuery[spans.size()]);
		SpanNearQuery snq=new SpanNearQuery(spanQueries,slop,inOrder);
		return snq;
	}

}
