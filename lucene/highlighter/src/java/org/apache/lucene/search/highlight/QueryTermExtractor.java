/*
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
package org.apache.lucene.search.highlight;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * Utility class used to extract the terms used in a query, plus any weights.
 * This class will not find terms for MultiTermQuery, TermRangeQuery and PrefixQuery classes
 * so the caller must pass a rewritten query (see Query.rewrite) to obtain a list of 
 * expanded terms. 
 * 
 */
public final class QueryTermExtractor
{

  /** for term extraction */
  private static final IndexSearcher EMPTY_INDEXSEARCHER;
  static {
    try {
      IndexReader emptyReader = new MultiReader();
      EMPTY_INDEXSEARCHER = new IndexSearcher(emptyReader);
      EMPTY_INDEXSEARCHER.setQueryCache(null);
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }

  /**
   * Extracts all terms texts of a given Query into an array of WeightedTerms
   *
   * @param query      Query to extract term texts from
   * @return an array of the terms used in a query, plus their weights.
   */
  public static final WeightedTerm[] getTerms(Query query)
  {
    return getTerms(query,false);
  }

  /**
   * Extracts all terms texts of a given Query into an array of WeightedTerms
   *
   * @param query      Query to extract term texts from
   * @param reader used to compute IDF which can be used to a) score selected fragments better
   * b) use graded highlights eg changing intensity of font color
   * @param fieldName the field on which Inverse Document Frequency (IDF) calculations are based
   * @return an array of the terms used in a query, plus their weights.
   */
  public static final WeightedTerm[] getIdfWeightedTerms(Query query, IndexReader reader, String fieldName)
  {
      WeightedTerm[] terms=getTerms(query,false, fieldName);
      int totalNumDocs=reader.maxDoc();
      for (int i = 0; i < terms.length; i++)
        {
          try
            {
                int docFreq=reader.docFreq(new Term(fieldName,terms[i].term));
                //IDF algorithm taken from DefaultSimilarity class
                float idf=(float)(Math.log(totalNumDocs/(double)(docFreq+1)) + 1.0);
                terms[i].weight*=idf;
            } 
          catch (IOException e)
            {
              //ignore
            }
        }
    return terms;
  }

  /**
   * Extracts all terms texts of a given Query into an array of WeightedTerms
   *
   * @param query      Query to extract term texts from
   * @param prohibited <code>true</code> to extract "prohibited" terms, too
   * @param fieldName  The fieldName used to filter query terms
   * @return an array of the terms used in a query, plus their weights.
   */
  public static final WeightedTerm[] getTerms(Query query, boolean prohibited, String fieldName)
  {
    HashSet<WeightedTerm> terms=new HashSet<>();
    getTerms(query, 1f, terms,prohibited,fieldName);
    return terms.toArray(new WeightedTerm[0]);
  }

  /**
   * Extracts all terms texts of a given Query into an array of WeightedTerms
   *
   * @param query      Query to extract term texts from
   * @param prohibited <code>true</code> to extract "prohibited" terms, too
   * @return an array of the terms used in a query, plus their weights.
   */
  public static final WeightedTerm[] getTerms(Query query, boolean prohibited)
  {
      return getTerms(query,prohibited,null);
  }

  private static final void getTerms(Query query, float boost, HashSet<WeightedTerm> terms, boolean prohibited, String fieldName) {
    try {
      if (query instanceof BoostQuery) {
        BoostQuery boostQuery = (BoostQuery) query;
        getTerms(boostQuery.getQuery(), boost * boostQuery.getBoost(), terms, prohibited, fieldName);
      } else if (query instanceof BooleanQuery)
        getTermsFromBooleanQuery((BooleanQuery) query, boost, terms, prohibited, fieldName);
      else if (query instanceof FilteredQuery)
        getTermsFromFilteredQuery((FilteredQuery) query, boost, terms, prohibited, fieldName);
      else {
        HashSet<Term> nonWeightedTerms = new HashSet<>();
        try {
          EMPTY_INDEXSEARCHER.createNormalizedWeight(query, false).extractTerms(nonWeightedTerms);
        } catch (IOException bogus) {
          throw new RuntimeException("Should not happen on an empty index", bogus);
        }
        for (Iterator<Term> iter = nonWeightedTerms.iterator(); iter.hasNext(); ) {
          Term term = iter.next();
          if ((fieldName == null) || (term.field().equals(fieldName))) {
            terms.add(new WeightedTerm(boost, term.text()));
          }
        }
      }
    } catch (UnsupportedOperationException ignore) {
      //this is non-fatal for our purposes
    }
  }

  /**
   * extractTerms is currently the only query-independent means of introspecting queries but it only reveals
   * a list of terms for that query - not the boosts each individual term in that query may or may not have.
   * "Container" queries such as BooleanQuery should be unwrapped to get at the boost info held
   * in each child element.
   * Some discussion around this topic here:
   * http://www.gossamer-threads.com/lists/lucene/java-dev/34208?search_string=introspection;#34208
   * Unfortunately there seemed to be limited interest in requiring all Query objects to implement
   * something common which would allow access to child queries so what follows here are query-specific
   * implementations for accessing embedded query elements.
   */
  private static final void getTermsFromBooleanQuery(BooleanQuery query, float boost, HashSet<WeightedTerm> terms, boolean prohibited, String fieldName)
  {
    for (BooleanClause clause : query)
    {
      if (prohibited || clause.getOccur()!=BooleanClause.Occur.MUST_NOT)
        getTerms(clause.getQuery(), boost, terms, prohibited, fieldName);
    }
  }

  private static void getTermsFromFilteredQuery(FilteredQuery query, float boost, HashSet<WeightedTerm> terms, boolean prohibited, String fieldName)
  {
    getTerms(query.getQuery(), boost, terms,prohibited,fieldName);
  }

}
