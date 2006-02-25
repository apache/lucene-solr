/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.similar.MoreLikeThisQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;


/**
 * @author maharwood
 */
public class LikeThisQueryBuilder implements QueryBuilder {

	private Analyzer analyzer;
	String defaultFieldNames [];
	int defaultMaxQueryTerms=20;
	int defaultMinTermFrequency=1;
	float defaultPercentTermsToMatch=30; //default is a 3rd of selected terms must match

	public LikeThisQueryBuilder(Analyzer analyzer,String [] defaultFieldNames)
	{
		this.analyzer=analyzer;
		this.defaultFieldNames=defaultFieldNames;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
	 */
	public Query getQuery(Element e) throws ParserException {
		String fieldsList=e.getAttribute("fieldNames"); //a comma-delimited list of fields
		String fields[]=defaultFieldNames;
		if((fieldsList!=null)&&(fieldsList.trim().length()>0))
		{
			fields=fieldsList.trim().split(",");
			//trim the fieldnames
			for (int i = 0; i < fields.length; i++) {
				fields[i]=fields[i].trim();
			}
		}
		MoreLikeThisQuery mlt=new MoreLikeThisQuery(DOMUtils.getText(e),fields,analyzer);
		mlt.setMaxQueryTerms(DOMUtils.getAttribute(e,"maxQueryTerms",defaultMaxQueryTerms));
		mlt.setMinTermFrequency(DOMUtils.getAttribute(e,"minTermFrequency",defaultMinTermFrequency));
		mlt.setPercentTermsToMatch(DOMUtils.getAttribute(e,"percentTermsToMatch",defaultPercentTermsToMatch)/100);

		mlt.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));

		return mlt;
	}



}
