package org.apache.lucene.queryparser.complexPhrase;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.Version;

/**
 * QueryParser which permits complex phrase query syntax eg "(john jon
 * jonathan~) peters*".
 * <p>
 * Performs potentially multiple passes over Query text to parse any nested
 * logic in PhraseQueries. - First pass takes any PhraseQuery content between
 * quotes and stores for subsequent pass. All other query content is parsed as
 * normal - Second pass parses any stored PhraseQuery content, checking all
 * embedded clauses are referring to the same field and therefore can be
 * rewritten as Span queries. All PhraseQuery clauses are expressed as
 * ComplexPhraseQuery objects
 * </p>
 * <p>
 * This could arguably be done in one pass using a new QueryParser but here I am
 * working within the constraints of the existing parser as a base class. This
 * currently simply feeds all phrase content through an analyzer to select
 * phrase terms - any "special" syntax such as * ~ * etc are not given special
 * status
 * </p>
 * 
 */
public class ComplexPhraseQueryParser extends QueryParser {
  private ArrayList<ComplexPhraseQuery> complexPhrases = null;

  private boolean isPass2ResolvingPhrases;

  private ComplexPhraseQuery currentPhraseQuery = null;

  public ComplexPhraseQueryParser(Version matchVersion, String f, Analyzer a) {
    super(matchVersion, f, a);
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, int slop) {
    ComplexPhraseQuery cpq = new ComplexPhraseQuery(field, queryText, slop);
    complexPhrases.add(cpq); // add to list of phrases to be parsed once
    // we
    // are through with this pass
    return cpq;
  }

  @Override
  public Query parse(String query) throws ParseException {
    if (isPass2ResolvingPhrases) {
      MultiTermQuery.RewriteMethod oldMethod = getMultiTermRewriteMethod();
      try {
        // Temporarily force BooleanQuery rewrite so that Parser will
        // generate visible
        // collection of terms which we can convert into SpanQueries.
        // ConstantScoreRewrite mode produces an
        // opaque ConstantScoreQuery object which cannot be interrogated for
        // terms in the same way a BooleanQuery can.
        // QueryParser is not guaranteed threadsafe anyway so this temporary
        // state change should not
        // present an issue
        setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        return super.parse(query);
      } finally {
        setMultiTermRewriteMethod(oldMethod);
      }
    }

    // First pass - parse the top-level query recording any PhraseQuerys
    // which will need to be resolved
    complexPhrases = new ArrayList<ComplexPhraseQuery>();
    Query q = super.parse(query);

    // Perform second pass, using this QueryParser to parse any nested
    // PhraseQueries with different
    // set of syntax restrictions (i.e. all fields must be same)
    isPass2ResolvingPhrases = true;
    try {
      for (Iterator<ComplexPhraseQuery> iterator = complexPhrases.iterator(); iterator.hasNext();) {
        currentPhraseQuery = iterator.next();
        // in each phrase, now parse the contents between quotes as a
        // separate parse operation
        currentPhraseQuery.parsePhraseElements(this);
      }
    } finally {
      isPass2ResolvingPhrases = false;
    }
    return q;
  }

  // There is No "getTermQuery throws ParseException" method to override so
  // unfortunately need
  // to throw a runtime exception here if a term for another field is embedded
  // in phrase query
  @Override
  protected Query newTermQuery(Term term) {
    if (isPass2ResolvingPhrases) {
      try {
        checkPhraseClauseIsForSameField(term.field());
      } catch (ParseException pe) {
        throw new RuntimeException("Error parsing complex phrase", pe);
      }
    }
    return super.newTermQuery(term);
  }

  // Helper method used to report on any clauses that appear in query syntax
  private void checkPhraseClauseIsForSameField(String field)
      throws ParseException {
    if (!field.equals(currentPhraseQuery.field)) {
      throw new ParseException("Cannot have clause for field \"" + field
          + "\" nested in phrase " + " for field \"" + currentPhraseQuery.field
          + "\"");
    }
  }

  @Override
  protected Query getWildcardQuery(String field, String termStr)
      throws ParseException {
    if (isPass2ResolvingPhrases) {
      checkPhraseClauseIsForSameField(field);
    }
    return super.getWildcardQuery(field, termStr);
  }

  @Override
  protected Query getRangeQuery(String field, String part1, String part2,
      boolean startInclusive, boolean endInclusive) throws ParseException {
    if (isPass2ResolvingPhrases) {
      checkPhraseClauseIsForSameField(field);
    }
    return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
  }

  @Override
  protected Query newRangeQuery(String field, String part1, String part2,
      boolean startInclusive, boolean endInclusive) {
    if (isPass2ResolvingPhrases) {
      // Must use old-style RangeQuery in order to produce a BooleanQuery
      // that can be turned into SpanOr clause
      TermRangeQuery rangeQuery = TermRangeQuery.newStringRange(field, part1, part2, startInclusive, endInclusive);
      rangeQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
      return rangeQuery;
    }
    return super.newRangeQuery(field, part1, part2, startInclusive, endInclusive);
  }

  @Override
  protected Query getFuzzyQuery(String field, String termStr,
      float minSimilarity) throws ParseException {
    if (isPass2ResolvingPhrases) {
      checkPhraseClauseIsForSameField(field);
    }
    return super.getFuzzyQuery(field, termStr, minSimilarity);
  }

  /*
   * Used to handle the query content in between quotes and produced Span-based
   * interpretations of the clauses.
   */
  static class ComplexPhraseQuery extends Query {

    String field;

    String phrasedQueryStringContents;

    int slopFactor;

    private Query contents;

    public ComplexPhraseQuery(String field, String phrasedQueryStringContents,
        int slopFactor) {
      super();
      this.field = field;
      this.phrasedQueryStringContents = phrasedQueryStringContents;
      this.slopFactor = slopFactor;
    }

    // Called by ComplexPhraseQueryParser for each phrase after the main
    // parse
    // thread is through
    protected void parsePhraseElements(QueryParser qp) throws ParseException {
      // TODO ensure that field-sensitivity is preserved ie the query
      // string below is parsed as
      // field+":("+phrasedQueryStringContents+")"
      // but this will need code in rewrite to unwrap the first layer of
      // boolean query
      contents = qp.parse(phrasedQueryStringContents);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      // ArrayList spanClauses = new ArrayList();
      if (contents instanceof TermQuery) {
        return contents;
      }
      // Build a sequence of Span clauses arranged in a SpanNear - child
      // clauses can be complex
      // Booleans e.g. nots and ors etc
      int numNegatives = 0;
      if (!(contents instanceof BooleanQuery)) {
        throw new IllegalArgumentException("Unknown query type \""
            + contents.getClass().getName()
            + "\" found in phrase query string \"" + phrasedQueryStringContents
            + "\"");
      }
      BooleanQuery bq = (BooleanQuery) contents;
      BooleanClause[] bclauses = bq.getClauses();
      SpanQuery[] allSpanClauses = new SpanQuery[bclauses.length];
      // For all clauses e.g. one* two~
      for (int i = 0; i < bclauses.length; i++) {
        // HashSet bclauseterms=new HashSet();
        Query qc = bclauses[i].getQuery();
        // Rewrite this clause e.g one* becomes (one OR onerous)
        qc = qc.rewrite(reader);
        if (bclauses[i].getOccur().equals(BooleanClause.Occur.MUST_NOT)) {
          numNegatives++;
        }

        if (qc instanceof BooleanQuery) {
          ArrayList<SpanQuery> sc = new ArrayList<SpanQuery>();
          addComplexPhraseClause(sc, (BooleanQuery) qc);
          if (sc.size() > 0) {
            allSpanClauses[i] = sc.get(0);
          } else {
            // Insert fake term e.g. phrase query was for "Fred Smithe*" and
            // there were no "Smithe*" terms - need to
            // prevent match on just "Fred".
            allSpanClauses[i] = new SpanTermQuery(new Term(field,
                "Dummy clause because no terms found - must match nothing"));
          }
        } else {
          if (qc instanceof TermQuery) {
            TermQuery tq = (TermQuery) qc;
            allSpanClauses[i] = new SpanTermQuery(tq.getTerm());
          } else {
            throw new IllegalArgumentException("Unknown query type \""
                + qc.getClass().getName()
                + "\" found in phrase query string \""
                + phrasedQueryStringContents + "\"");
          }

        }
      }
      if (numNegatives == 0) {
        // The simple case - no negative elements in phrase
        return new SpanNearQuery(allSpanClauses, slopFactor, true);
      }
      // Complex case - we have mixed positives and negatives in the
      // sequence.
      // Need to return a SpanNotQuery
      ArrayList<SpanQuery> positiveClauses = new ArrayList<SpanQuery>();
      for (int j = 0; j < allSpanClauses.length; j++) {
        if (!bclauses[j].getOccur().equals(BooleanClause.Occur.MUST_NOT)) {
          positiveClauses.add(allSpanClauses[j]);
        }
      }

      SpanQuery[] includeClauses = positiveClauses
          .toArray(new SpanQuery[positiveClauses.size()]);

      SpanQuery include = null;
      if (includeClauses.length == 1) {
        include = includeClauses[0]; // only one positive clause
      } else {
        // need to increase slop factor based on gaps introduced by
        // negatives
        include = new SpanNearQuery(includeClauses, slopFactor + numNegatives,
            true);
      }
      // Use sequence of positive and negative values as the exclude.
      SpanNearQuery exclude = new SpanNearQuery(allSpanClauses, slopFactor,
          true);
      SpanNotQuery snot = new SpanNotQuery(include, exclude);
      return snot;
    }

    private void addComplexPhraseClause(List<SpanQuery> spanClauses, BooleanQuery qc) {
      ArrayList<SpanQuery> ors = new ArrayList<SpanQuery>();
      ArrayList<SpanQuery> nots = new ArrayList<SpanQuery>();
      BooleanClause[] bclauses = qc.getClauses();

      // For all clauses e.g. one* two~
      for (int i = 0; i < bclauses.length; i++) {
        Query childQuery = bclauses[i].getQuery();

        // select the list to which we will add these options
        ArrayList<SpanQuery> chosenList = ors;
        if (bclauses[i].getOccur() == BooleanClause.Occur.MUST_NOT) {
          chosenList = nots;
        }

        if (childQuery instanceof TermQuery) {
          TermQuery tq = (TermQuery) childQuery;
          SpanTermQuery stq = new SpanTermQuery(tq.getTerm());
          stq.setBoost(tq.getBoost());
          chosenList.add(stq);
        } else if (childQuery instanceof BooleanQuery) {
          BooleanQuery cbq = (BooleanQuery) childQuery;
          addComplexPhraseClause(chosenList, cbq);
        } else {
          // TODO alternatively could call extract terms here?
          throw new IllegalArgumentException("Unknown query type:"
              + childQuery.getClass().getName());
        }
      }
      if (ors.size() == 0) {
        return;
      }
      SpanOrQuery soq = new SpanOrQuery(ors
          .toArray(new SpanQuery[ors.size()]));
      if (nots.size() == 0) {
        spanClauses.add(soq);
      } else {
        SpanOrQuery snqs = new SpanOrQuery(nots
            .toArray(new SpanQuery[nots.size()]));
        SpanNotQuery snq = new SpanNotQuery(soq, snqs);
        spanClauses.add(snq);
      }
    }

    @Override
    public String toString(String field) {
      return "\"" + phrasedQueryStringContents + "\"";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((field == null) ? 0 : field.hashCode());
      result = prime
          * result
          + ((phrasedQueryStringContents == null) ? 0
              : phrasedQueryStringContents.hashCode());
      result = prime * result + slopFactor;
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
      if (!super.equals(obj)) {
        return false;
      }
      ComplexPhraseQuery other = (ComplexPhraseQuery) obj;
      if (field == null) {
        if (other.field != null)
          return false;
      } else if (!field.equals(other.field))
        return false;
      if (phrasedQueryStringContents == null) {
        if (other.phrasedQueryStringContents != null)
          return false;
      } else if (!phrasedQueryStringContents
          .equals(other.phrasedQueryStringContents))
        return false;
      if (slopFactor != other.slopFactor)
        return false;
      return true;
    }
  }
}
