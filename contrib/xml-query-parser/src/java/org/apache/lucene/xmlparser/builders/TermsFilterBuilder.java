package org.apache.lucene.xmlparser.builders;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermsFilter;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.FilterBuilder;
import org.apache.lucene.xmlparser.ParserException;
import org.w3c.dom.Element;


/**
 * @author maharwood
 *
 * @
 */
public class TermsFilterBuilder implements FilterBuilder
{
	Analyzer analyzer;
	
	/**
	 * @param analyzer
	 */
	public TermsFilterBuilder(Analyzer analyzer)
	{
		this.analyzer = analyzer;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.lucene.xmlparser.FilterBuilder#process(org.w3c.dom.Element)
	 */
	public Filter getFilter(Element e) throws ParserException
	{
		TermsFilter tf = new TermsFilter();
		String text = DOMUtils.getNonBlankTextOrFail(e);
		String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
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
				tf.addTerm(term);
				token = ts.next();
			}
		} 
		catch (IOException ioe)
		{
			throw new RuntimeException("Error constructing terms from index:"
					+ ioe);
		}
		return tf;
	}
}
