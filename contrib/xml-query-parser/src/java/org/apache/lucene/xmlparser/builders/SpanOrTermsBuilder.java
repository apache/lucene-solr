package org.apache.lucene.xmlparser.builders;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;

public class SpanOrTermsBuilder extends SpanBuilderBase
{
    Analyzer analyzer;
    
    
    /**
     * @param analyzer
     */
    public SpanOrTermsBuilder(Analyzer analyzer)
    {
        super();
        this.analyzer = analyzer;
    }
	public SpanQuery getSpanQuery(Element e) throws ParserException
	{
 		String fieldName=DOMUtils.getAttributeWithInheritanceOrFail(e,"fieldName");
 		String value=DOMUtils.getNonBlankTextOrFail(e);
		
		try
		{
			ArrayList clausesList=new ArrayList();
			TokenStream ts=analyzer.tokenStream(fieldName,new StringReader(value));
			Token token=ts.next();
			while(token!=null)
			{
			    SpanTermQuery stq=new SpanTermQuery(new Term(fieldName,token.termText()));
			    clausesList.add(stq);
				token=ts.next();		    
			}
			SpanOrQuery soq=new SpanOrQuery((SpanQuery[]) clausesList.toArray(new SpanQuery[clausesList.size()]));
			soq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));
			return soq;
		}
		catch(IOException ioe)
		{
		    throw new ParserException("IOException parsing value:"+value);
		}
	}

}
