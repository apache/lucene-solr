/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;


/**
 * Builds a BooleanQuery from all of the terms found in the XML element using the choice of analyzer
 * @author maharwood
 */
public class TermsQueryBuilder implements QueryBuilder {

	Analyzer analyzer;

		
	public TermsQueryBuilder(Analyzer analyzer)
	{
		this.analyzer = analyzer;
	}



	public Query getQuery(Element e) throws ParserException {
		
        String fieldName=DOMUtils.getAttributeWithInheritanceOrFail(e,"fieldName");
 		String text=DOMUtils.getNonBlankTextOrFail(e);
 		
		BooleanQuery bq=new BooleanQuery(DOMUtils.getAttribute(e,"disableCoord",false));
		bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(e,"minimumNumberShouldMatch",0));
		TokenStream ts = analyzer.tokenStream(fieldName, new StringReader(text));
		try
		{
			Token token = ts.next();
			Term term = null;
			while (token != null)
			{
				if (term == null)
				{
					term = new Term(fieldName, token.termText());
				} else
				{
//					 create from previous to save fieldName.intern overhead
					term = term.createTerm(token.termText()); 
				}
				bq.add(new BooleanClause(new TermQuery(term),BooleanClause.Occur.SHOULD));
				token = ts.next();
			}
		} 
		catch (IOException ioe)
		{
			throw new RuntimeException("Error constructing terms from index:"
					+ ioe);
		}
  		bq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));

  		return bq;
		
	}

}
