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
package org.apache.lucene.classification.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.search.MaxNonCompetitiveBoostAttribute;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

/**
 * Simplification of FuzzyLikeThisQuery, to be used in the context of KNN classification.
 */
public class NearestFuzzyQuery extends Query {

  private final ArrayList<FieldVals> fieldVals = new ArrayList<>();
  private final Analyzer analyzer;

  // fixed parameters
  private static final int MAX_VARIANTS_PER_TERM = 50;
  private static final float MIN_SIMILARITY = 1f;
  private static final int PREFIX_LENGTH = 2;
  private static final int MAX_NUM_TERMS = 300;

  /**
   * Default constructor
   *
   * @param analyzer the analyzer used to proecss the query text
   */
  public NearestFuzzyQuery(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  static class FieldVals {
    final String queryString;
    final String fieldName;
    final int maxEdits;
    final int prefixLength;

    FieldVals(String name, int maxEdits, String queryString) {
      this.fieldName = name;
      this.maxEdits = maxEdits;
      this.queryString = queryString;
      this.prefixLength = NearestFuzzyQuery.PREFIX_LENGTH;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((fieldName == null) ? 0 : fieldName.hashCode());
      result = prime * result + maxEdits;
      result = prime * result + prefixLength;
      result = prime * result
          + ((queryString == null) ? 0 : queryString.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FieldVals other = (FieldVals) obj;
      if (fieldName == null) {
        if (other.fieldName != null)
          return false;
      } else if (!fieldName.equals(other.fieldName))
        return false;
      if (maxEdits != other.maxEdits) {
        return false;
      }
      if (prefixLength != other.prefixLength)
        return false;
      if (queryString == null) {
        if (other.queryString != null)
          return false;
      } else if (!queryString.equals(other.queryString))
        return false;
      return true;
    }


  }

  /**
   * Adds user input for "fuzzification"
   *
   * @param queryString The string which will be parsed by the analyzer and for which fuzzy variants will be parsed
   */
  public void addTerms(String queryString, String fieldName) {
    int maxEdits = (int) MIN_SIMILARITY;
    if (maxEdits != MIN_SIMILARITY) {
      throw new IllegalArgumentException("MIN_SIMILARITY must integer value between 0 and " + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + ", inclusive; got " + MIN_SIMILARITY);
    }
    fieldVals.add(new FieldVals(fieldName, maxEdits, queryString));
  }


  private void addTerms(IndexReader reader, FieldVals f, ScoreTermQueue q) throws IOException {
    if (f.queryString == null) return;
    final Terms terms = MultiFields.getTerms(reader, f.fieldName);
    if (terms == null) {
      return;
    }
    try (TokenStream ts = analyzer.tokenStream(f.fieldName, f.queryString)) {
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);

      int corpusNumDocs = reader.numDocs();
      HashSet<String> processedTerms = new HashSet<>();
      ts.reset();
      while (ts.incrementToken()) {
        String term = termAtt.toString();
        if (!processedTerms.contains(term)) {
          processedTerms.add(term);
          ScoreTermQueue variantsQ = new ScoreTermQueue(MAX_VARIANTS_PER_TERM); //maxNum variants considered for any one term
          float minScore = 0;
          Term startTerm = new Term(f.fieldName, term);
          AttributeSource atts = new AttributeSource();
          MaxNonCompetitiveBoostAttribute maxBoostAtt =
              atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
          FuzzyTermsEnum fe = new FuzzyTermsEnum(terms, atts, startTerm, f.maxEdits, f.prefixLength, true);
          //store the df so all variants use same idf
          int df = reader.docFreq(startTerm);
          int numVariants = 0;
          int totalVariantDocFreqs = 0;
          BytesRef possibleMatch;
          BoostAttribute boostAtt =
              fe.attributes().addAttribute(BoostAttribute.class);
          while ((possibleMatch = fe.next()) != null) {
            numVariants++;
            totalVariantDocFreqs += fe.docFreq();
            float score = boostAtt.getBoost();
            if (variantsQ.size() < MAX_VARIANTS_PER_TERM || score > minScore) {
              ScoreTerm st = new ScoreTerm(new Term(startTerm.field(), BytesRef.deepCopyOf(possibleMatch)), score, startTerm);
              variantsQ.insertWithOverflow(st);
              minScore = variantsQ.top().score; // maintain minScore
            }
            maxBoostAtt.setMaxNonCompetitiveBoost(variantsQ.size() >= MAX_VARIANTS_PER_TERM ? minScore : Float.NEGATIVE_INFINITY);
          }

          if (numVariants > 0) {
            int avgDf = totalVariantDocFreqs / numVariants;
            if (df == 0)//no direct match we can use as df for all variants
            {
              df = avgDf; //use avg df of all variants
            }

            // take the top variants (scored by edit distance) and reset the score
            // to include an IDF factor then add to the global queue for ranking
            // overall top query terms
            int size = variantsQ.size();
            for (int i = 0; i < size; i++) {
              ScoreTerm st = variantsQ.pop();
              if (st != null) {
                st.score = (st.score * st.score) * idf(df, corpusNumDocs);
                q.insertWithOverflow(st);
              }
            }
          }
        }
      }
      ts.end();
    }
  }

  private float idf(int docFreq, int docCount) {
    return (float)(Math.log((docCount+1)/(double)(docFreq+1)) + 1.0);
  }

  private Query newTermQuery(IndexReader reader, Term term) throws IOException {
    // we build an artificial TermContext that will give an overall df and ttf
    // equal to 1
    TermContext context = new TermContext(reader.getContext());
    for (LeafReaderContext leafContext : reader.leaves()) {
      Terms terms = leafContext.reader().terms(term.field());
      if (terms != null) {
        TermsEnum termsEnum = terms.iterator();
        if (termsEnum.seekExact(term.bytes())) {
          int freq = 1 - context.docFreq(); // we want the total df and ttf to be 1
          context.register(termsEnum.termState(), leafContext.ord, freq, freq);
        }
      }
    }
    return new TermQuery(term, context);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    ScoreTermQueue q = new ScoreTermQueue(MAX_NUM_TERMS);
    //load up the list of possible terms
    for (FieldVals f : fieldVals) {
      addTerms(reader, f, q);
    }

    BooleanQuery.Builder bq = new BooleanQuery.Builder();

    //create BooleanQueries to hold the variants for each token/field pair and ensure it
    // has no coord factor
    //Step 1: sort the termqueries by term/field
    HashMap<Term, ArrayList<ScoreTerm>> variantQueries = new HashMap<>();
    int size = q.size();
    for (int i = 0; i < size; i++) {
      ScoreTerm st = q.pop();
      if (st != null) {
        ArrayList<ScoreTerm> l = variantQueries.computeIfAbsent(st.fuzziedSourceTerm, k -> new ArrayList<>());
        l.add(st);
      }
    }
    //Step 2: Organize the sorted termqueries into zero-coord scoring boolean queries
    for (ArrayList<ScoreTerm> variants : variantQueries.values()) {
      if (variants.size() == 1) {
        //optimize where only one selected variant
        ScoreTerm st = variants.get(0);
        Query tq = newTermQuery(reader, st.term);
        // set the boost to a mix of IDF and score
        bq.add(new BoostQuery(tq, st.score), BooleanClause.Occur.SHOULD);
      } else {
        BooleanQuery.Builder termVariants = new BooleanQuery.Builder();
        for (ScoreTerm st : variants) {
          // found a match
          Query tq = newTermQuery(reader, st.term);
          // set the boost using the ScoreTerm's score
          termVariants.add(new BoostQuery(tq, st.score), BooleanClause.Occur.SHOULD);          // add to query
        }
        bq.add(termVariants.build(), BooleanClause.Occur.SHOULD);          // add to query
      }
    }
    //TODO possible alternative step 3 - organize above booleans into a new layer of field-based
    // booleans with a minimum-should-match of NumFields-1?
    return bq.build();
  }

  //Holds info for a fuzzy term variant - initially score is set to edit distance (for ranking best
  // term variants) then is reset with IDF for use in ranking against all other
  // terms/fields
  private static class ScoreTerm {
    public final Term term;
    public float score;
    final Term fuzziedSourceTerm;

    ScoreTerm(Term term, float score, Term fuzziedSourceTerm) {
      this.term = term;
      this.score = score;
      this.fuzziedSourceTerm = fuzziedSourceTerm;
    }
  }

  private static class ScoreTermQueue extends PriorityQueue<ScoreTerm> {
    ScoreTermQueue(int size) {
      super(size);
    }

    /* (non-Javadoc)
     * @see org.apache.lucene.util.PriorityQueue#lessThan(java.lang.Object, java.lang.Object)
     */
    @Override
    protected boolean lessThan(ScoreTerm termA, ScoreTerm termB) {
      if (termA.score == termB.score)
        return termA.term.compareTo(termB.term) > 0;
      else
        return termA.score < termB.score;
    }

  }

  @Override
  public String toString(String field) {
    return null;
  }

  @Override
  public int hashCode() {
    int prime = 31;
    int result = classHash();
    result = prime * result + Objects.hashCode(analyzer);
    result = prime * result + Objects.hashCode(fieldVals);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
        equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(NearestFuzzyQuery other) {
    return Objects.equals(analyzer, other.analyzer) &&
        Objects.equals(fieldVals, other.fieldVals);
  }

}
