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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
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

		FSDirectory directory = FSDirectory.open(new File(args[0]));
		IndexSearcher searcher = new IndexSearcher(directory, true);

		String word = args[1];
		Query query = new TermQuery(new Term(Syns2Index.F_WORD, word));
		TotalHitCountCollector countingCollector = new TotalHitCountCollector();
		searcher.search(query, countingCollector);

		if (countingCollector.getTotalHits() == 0) {
			System.out.println("No synonyms found for " + word);
		} else {
			System.out.println("Synonyms found for \"" + word + "\":");
		}

		ScoreDoc[] hits = searcher.search(query, countingCollector.getTotalHits()).scoreDocs;
		
		for (int i = 0; i < hits.length; i++) {
			Document doc = searcher.doc(hits[i].doc);

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
								IndexSearcher syns,
								Analyzer a,
								final String field,
								final float boost)
		throws IOException
	{
		final Set<String> already = new HashSet<String>(); // avoid dups		
		List<String> top = new LinkedList<String>(); // needs to be separately listed..

		// [1] Parse query into separate words so that when we expand we can avoid dups
		TokenStream ts = a.reusableTokenStream( field, new StringReader( query));
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    
		while (ts.incrementToken()) {
			String word = termAtt.toString();
			if ( already.add( word))
				top.add( word);
		}
		final BooleanQuery tmp = new BooleanQuery();
		
		// [2] form query
		Iterator<String> it = top.iterator();
		while ( it.hasNext())
		{
			// [2a] add to level words in
			String word = it.next();
			TermQuery tq = new TermQuery( new Term( field, word));
			tmp.add( tq, BooleanClause.Occur.SHOULD);

			// [2b] add in unique synonums
			syns.search(new TermQuery( new Term(Syns2Index.F_WORD, word)), new Collector() {
			  IndexReader reader;
			  
        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }

        @Override
        public void collect(int doc) throws IOException {
          Document d = reader.document(doc);
          String[] values = d.getValues( Syns2Index.F_SYN);
          for ( int j = 0; j < values.length; j++)
          {
            String syn = values[ j];
            if ( already.add( syn))
            {
              TermQuery tq = new TermQuery( new Term( field, syn));
              if ( boost > 0) // else keep normal 1.0
                tq.setBoost( boost);
              tmp.add( tq, BooleanClause.Occur.SHOULD); 
            }
          }
        }

        @Override
        public void setNextReader(AtomicReaderContext context)
            throws IOException {
          this.reader = context.reader;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {}
			});
		}


		return tmp;
	}
								
}
