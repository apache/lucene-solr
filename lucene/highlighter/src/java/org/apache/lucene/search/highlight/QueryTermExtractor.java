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
import java.util.Set;
import java.util.function.Predicate;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

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
                //IDF algorithm taken from ClassicSimilarity class
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
  public static WeightedTerm[] getTerms(Query query, boolean prohibited, String fieldName) {
    HashSet<WeightedTerm> terms = new HashSet<>();
    Predicate<String> fieldSelector = fieldName == null ? f -> true : fieldName::equals;
    query.visit(new BoostedTermExtractor(1, terms, prohibited, fieldSelector));
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

  private static class BoostedTermExtractor extends QueryVisitor {

    final float boost;
    final Set<WeightedTerm> terms;
    final boolean includeProhibited;
    final Predicate<String> fieldSelector;

    private BoostedTermExtractor(float boost, Set<WeightedTerm> terms, boolean includeProhibited,
                                 Predicate<String> fieldSelector) {
      this.boost = boost;
      this.terms = terms;
      this.includeProhibited = includeProhibited;
      this.fieldSelector = fieldSelector;
    }

    @Override
    public boolean acceptField(String field) {
      return fieldSelector.test(field);
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      for (Term term : terms) {
        this.terms.add(new WeightedTerm(boost, term.text()));
      }
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (parent instanceof BoostQuery) {
        float newboost = boost * ((BoostQuery)parent).getBoost();
        return new BoostedTermExtractor(newboost, terms, includeProhibited, fieldSelector);
      }
      if (occur == BooleanClause.Occur.MUST_NOT && includeProhibited == false) {
        return QueryVisitor.EMPTY_VISITOR;
      }
      return this;
    }

  }

}
