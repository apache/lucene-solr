package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.FuzzyLikeThisQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class FuzzyLikeThisQueryBuilder implements QueryBuilder
{
	int defaultMaxNumTerms=50;
	float defaultMinSimilarity=0.5f;
	int defaultPrefixLength=1;
	boolean defaultIgnoreTF=false;
	private Analyzer analyzer;
	
	public FuzzyLikeThisQueryBuilder(Analyzer analyzer)
	{
		this.analyzer=analyzer;
	}

	public Query getQuery(Element e) throws ParserException
	{
		NodeList nl = e.getElementsByTagName("Field");
		int maxNumTerms=DOMUtils.getAttribute(e,"maxNumTerms",defaultMaxNumTerms);
		FuzzyLikeThisQuery fbq=new FuzzyLikeThisQuery(maxNumTerms,analyzer);
		fbq.setIgnoreTF(DOMUtils.getAttribute(e,"ignoreTF",defaultIgnoreTF));
		for(int i=0;i<nl.getLength();i++)
		{
			Element fieldElem=(Element) nl.item(i);
			float minSimilarity=DOMUtils.getAttribute(fieldElem,"minSimilarity",defaultMinSimilarity);
			int prefixLength=DOMUtils.getAttribute(fieldElem,"prefixLength",defaultPrefixLength);
			String fieldName=DOMUtils.getAttributeWithInheritance(fieldElem,"fieldName");
			
			String value=DOMUtils.getText(fieldElem);
			fbq.addTerms(value,fieldName,minSimilarity,prefixLength);
		}
		fbq.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));

		return fbq;
	}

}
