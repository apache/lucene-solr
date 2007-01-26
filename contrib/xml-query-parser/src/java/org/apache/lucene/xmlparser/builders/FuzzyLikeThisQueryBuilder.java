package org.apache.lucene.xmlparser.builders;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.FuzzyLikeThisQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.xmlparser.DOMUtils;
import org.apache.lucene.xmlparser.ParserException;
import org.apache.lucene.xmlparser.QueryBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

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
