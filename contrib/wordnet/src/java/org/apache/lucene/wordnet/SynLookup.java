package org.apache.lucene.wordnet;

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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;


/**
 * Test program to look up synonyms.
 */
public class SynLookup {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println(
							   "java org.apache.lucene.wordnet.SynLookup <index path> <word>");
		}

		FSDirectory directory = FSDirectory.getDirectory(args[0], false);
		IndexSearcher searcher = new IndexSearcher(directory);

		String word = args[1];
		Hits hits = searcher.search(
									new TermQuery(new Term(Syns2Index.F_WORD, word)));

		if (hits.length() == 0) {
			System.out.println("No synonyms found for " + word);
		} else {
			System.out.println("Synonyms found for \"" + word + "\":");
		}

		for (int i = 0; i < hits.length(); i++) {
			Document doc = hits.doc(i);

			String[] values = doc.getValues(Syns2Index.F_SYN);

			for (int j = 0; j < values.length; j++) {
				System.out.println(values[j]);
			}
		}

		searcher.close();
		directory.close();
	}


	/**
	 * Perform synonym expansion on a query.
	 *
	 * @param query
	 * @param syns
	 * @param a
	 * @param field
	 * @param boost
	 */ 
	public static Query expand( String query,
								Searcher syns,
								Analyzer a,
								String field,
								float boost)
		throws IOException
	{
		Set already = new HashSet(); // avoid dups		
		List top = new LinkedList(); // needs to be separately listed..

		// [1] Parse query into separate words so that when we expand we can avoid dups
		TokenStream ts = a.tokenStream( field, new StringReader( query));
                final Token reusableToken = new Token();
		for (Token nextToken = ts.next(reusableToken); nextToken != null; nextToken = ts.next(reusableToken)) {
			String word = nextToken.term();
			if ( already.add( word))
				top.add( word);
		}
		BooleanQuery tmp = new BooleanQuery();
		
		// [2] form query
		Iterator it = top.iterator();
		while ( it.hasNext())
		{
			// [2a] add to level words in
			String word = (String) it.next();
			TermQuery tq = new TermQuery( new Term( field, word));
			tmp.add( tq, BooleanClause.Occur.SHOULD);

			// [2b] add in unique synonums
			Hits hits = syns.search( new TermQuery( new Term(Syns2Index.F_WORD, word)));
			for (int i = 0; i < hits.length(); i++)
			{
				Document doc = hits.doc(i);
				String[] values = doc.getValues( Syns2Index.F_SYN);
				for ( int j = 0; j < values.length; j++)
				{
					String syn = values[ j];
					if ( already.add( syn))
					{
						tq = new TermQuery( new Term( field, syn));
						if ( boost > 0) // else keep normal 1.0
							tq.setBoost( boost);
						tmp.add( tq, BooleanClause.Occur.SHOULD); 
					}
				}
			}
		}


		return tmp;
	}
								
}
