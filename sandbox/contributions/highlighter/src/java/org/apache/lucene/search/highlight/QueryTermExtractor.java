package org.apache.lucene.search.highlight;
/**
 * Copyright 2002-2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashSet;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Utility class used to extract the terms used in a query, plus any weights.
 * This class will not find terms for MultiTermQuery, RangeQuery and PrefixQuery classes
 * so the caller must pass a rewritten query (see query.rewrite) to obtain a list of 
 * expanded terms. 
 * 
 */
public final class QueryTermExtractor
{

	/**
	 * Extracts all terms texts of a given Query into an array of WeightedTerms
	 *
	 * @param query      Query to extract term texts from
	 * @return an array of the terms used in a query, plus their weights.
	 * @throws IOException
	 */
	public static final WeightedTerm[] getTerms(Query query) 
	{
		return getTerms(query,false);
	}


	/**
	 * Extracts all terms texts of a given Query into an array of WeightedTerms
	 *
	 * @param query      Query to extract term texts from
	 * @param prohibited <code>true</code> to extract "prohibited" terms, too
     * @return an array of the terms used in a query, plus their weights.
     * @throws IOException
     */
	public static final WeightedTerm[] getTerms(Query query, boolean prohibited) 
	{
		HashSet terms=new HashSet();
		getTerms(query,terms,prohibited);
		return (WeightedTerm[]) terms.toArray(new WeightedTerm[0]);
	}

	private static final void getTerms(Query query, HashSet terms,boolean prohibited) 
	{
		if (query instanceof BooleanQuery)
			getTermsFromBooleanQuery((BooleanQuery) query, terms, prohibited);
		else
			if (query instanceof PhraseQuery)
				getTermsFromPhraseQuery((PhraseQuery) query, terms);
			else
				if (query instanceof TermQuery)
					getTermsFromTermQuery((TermQuery) query, terms);
//				else
//					if ((query instanceof PrefixQuery)
//						|| (query instanceof RangeQuery)
//						|| (query instanceof MultiTermQuery))
//					{
//						//client should call rewrite BEFORE calling highlighter
//						//						Query expandedQuery = rewrite(reader, query);
//						//				getTerms(reader, expandedQuery, terms, prohibited);
//					}
	}

	private static final void getTermsFromBooleanQuery(BooleanQuery query, HashSet terms, boolean prohibited)
	{
		BooleanClause[] queryClauses = query.getClauses();
		int i;

		for (i = 0; i < queryClauses.length; i++)
		{
			if (prohibited || !queryClauses[i].prohibited)
				getTerms(queryClauses[i].query, terms, prohibited);
		}
	}

	private static final void getTermsFromPhraseQuery(PhraseQuery query, HashSet terms)
	{
		Term[] queryTerms = query.getTerms();
		int i;

		for (i = 0; i < queryTerms.length; i++)
		{
			terms.add(new WeightedTerm(query.getBoost(),queryTerms[i].text()));
		}
	}

	private static final void getTermsFromTermQuery(TermQuery query, HashSet terms)
	{
		terms.add(new WeightedTerm(query.getBoost(),query.getTerm().text()));
	}


}
