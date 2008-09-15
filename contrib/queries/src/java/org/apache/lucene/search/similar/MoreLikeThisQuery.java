/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.search.similar;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similar.MoreLikeThis;

/**
 * A simple wrapper for MoreLikeThis for use in scenarios where a Query object is required eg
 * in custom QueryParser extensions. At query.rewrite() time the reader is used to construct the
 * actual MoreLikeThis object and obtain the real Query object.
 */
public class MoreLikeThisQuery extends Query
{

    
    private String likeText;
    private String[] moreLikeFields;
    private Analyzer analyzer;
    float percentTermsToMatch=0.3f;
    int minTermFrequency=1;
    int maxQueryTerms=5;
    Set stopWords=null;
	int minDocFreq=-1;
    
    
    /**
     * @param moreLikeFields
     */
    public MoreLikeThisQuery(String likeText, String[] moreLikeFields, Analyzer analyzer)
    {
        this.likeText=likeText;
        this.moreLikeFields=moreLikeFields;
        this.analyzer=analyzer;
    }
    
    public Query rewrite(IndexReader reader) throws IOException
    {
        MoreLikeThis mlt=new MoreLikeThis(reader);
        
        mlt.setFieldNames(moreLikeFields);
        mlt.setAnalyzer(analyzer);
        mlt.setMinTermFreq(minTermFrequency);
        if(minDocFreq>=0)
        {
        	mlt.setMinDocFreq(minDocFreq);
        }        
        mlt.setMaxQueryTerms(maxQueryTerms);
        mlt.setStopWords(stopWords);
        BooleanQuery bq= (BooleanQuery) mlt.like(new ByteArrayInputStream(likeText.getBytes()));        
        BooleanClause[] clauses = bq.getClauses();
        //make at least half the terms match
        bq.setMinimumNumberShouldMatch((int)(clauses.length*percentTermsToMatch));
        return bq;
    }
    /* (non-Javadoc)
     * @see org.apache.lucene.search.Query#toString(java.lang.String)
     */
    public String toString(String field)
    {       
        return "like:"+likeText;
    }

	public float getPercentTermsToMatch() {
		return percentTermsToMatch;
	}
	public void setPercentTermsToMatch(float percentTermsToMatch) {
		this.percentTermsToMatch = percentTermsToMatch;
	}

	public Analyzer getAnalyzer()
	{
		return analyzer;
	}

	public void setAnalyzer(Analyzer analyzer)
	{
		this.analyzer = analyzer;
	}

	public String getLikeText()
	{
		return likeText;
	}

	public void setLikeText(String likeText)
	{
		this.likeText = likeText;
	}

	public int getMaxQueryTerms()
	{
		return maxQueryTerms;
	}

	public void setMaxQueryTerms(int maxQueryTerms)
	{
		this.maxQueryTerms = maxQueryTerms;
	}

	public int getMinTermFrequency()
	{
		return minTermFrequency;
	}

	public void setMinTermFrequency(int minTermFrequency)
	{
		this.minTermFrequency = minTermFrequency;
	}

	public String[] getMoreLikeFields()
	{
		return moreLikeFields;
	}

	public void setMoreLikeFields(String[] moreLikeFields)
	{
		this.moreLikeFields = moreLikeFields;
	}
    public Set getStopWords()
    {
        return stopWords;
    }
    public void setStopWords(Set stopWords)
    {
        this.stopWords = stopWords;
    }

	public int getMinDocFreq()
	{
		return minDocFreq;
	}

	public void setMinDocFreq(int minDocFreq)
	{
		this.minDocFreq = minDocFreq;
	}
}
