/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.xmlparser.builders;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.similar.MoreLikeThisQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
		
		//Parse any "stopWords" attribute
		//TODO MoreLikeThis needs to ideally have per-field stopWords lists - until then 
		//I use all analyzers/fields to generate multi-field compatible stop list
		String stopWords=e.getAttribute("stopWords");
		Set stopWordsSet=null;
		if((stopWords!=null)&&(fields!=null))
		{
		    stopWordsSet=new HashSet();
                    final Token reusableToken = new Token();
		    for (int i = 0; i < fields.length; i++)
            {
                TokenStream ts = analyzer.tokenStream(fields[i],new StringReader(stopWords));
                try
                {
	                for (Token nextToken = ts.next(reusableToken); nextToken != null; nextToken = ts.next(reusableToken)) {
	                    stopWordsSet.add(nextToken.term());
	                }
                }
                catch(IOException ioe)
                {
                    throw new ParserException("IoException parsing stop words list in "
                            +getClass().getName()+":"+ioe.getLocalizedMessage());
                }
            }
		}
		
		
		MoreLikeThisQuery mlt=new MoreLikeThisQuery(DOMUtils.getText(e),fields,analyzer);
		mlt.setMaxQueryTerms(DOMUtils.getAttribute(e,"maxQueryTerms",defaultMaxQueryTerms));
		mlt.setMinTermFrequency(DOMUtils.getAttribute(e,"minTermFrequency",defaultMinTermFrequency));
		mlt.setPercentTermsToMatch(DOMUtils.getAttribute(e,"percentTermsToMatch",defaultPercentTermsToMatch)/100);
		mlt.setStopWords(stopWordsSet);
		int minDocFreq=DOMUtils.getAttribute(e,"minDocFreq",-1);
		if(minDocFreq>=0)
		{
			mlt.setMinDocFreq(minDocFreq);
		}

		mlt.setBoost(DOMUtils.getAttribute(e,"boost",1.0f));

		return mlt;
	}



}
