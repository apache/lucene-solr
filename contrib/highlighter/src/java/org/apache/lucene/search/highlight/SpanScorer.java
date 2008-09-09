package org.apache.lucene.search.highlight;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;


/**
 * {@link Scorer} implementation which scores text fragments by the number of
 * unique query terms found. This class converts appropriate Querys to
 * SpanQuerys and attempts to score only those terms that participated in
 * generating the 'hit' on the document.
 */
public class SpanScorer implements Scorer {
  private float totalScore;
  private Set foundTerms;
  private Map fieldWeightedSpanTerms;
  private float maxTermWeight;
  private int position = -1;
  private String defaultField;
  private static boolean highlightCnstScrRngQuery;

  /**
   * @param query
   *            Query to use for highlighting
   * @param field
   *            Field to highlight - pass null to ignore fields
   * @param tokenStream
   *            of source text to be highlighted
   * @throws IOException
   */
  public SpanScorer(Query query, String field,
    CachingTokenFilter cachingTokenFilter) throws IOException {
    init(query, field, cachingTokenFilter, null);
  }

  /**
   * @param query
   *            Query to use for highlighting
   * @param field
   *            Field to highlight - pass null to ignore fields
   * @param tokenStream
   *            of source text to be highlighted
   * @param reader
   * @throws IOException
   */
  public SpanScorer(Query query, String field,
    CachingTokenFilter cachingTokenFilter, IndexReader reader)
    throws IOException {
    init(query, field, cachingTokenFilter, reader);
  }

  /**
   * As above, but with ability to pass in an <tt>IndexReader</tt>
   */
  public SpanScorer(Query query, String field,
    CachingTokenFilter cachingTokenFilter, IndexReader reader, String defaultField)
    throws IOException {
    this.defaultField = defaultField.intern();
    init(query, field, cachingTokenFilter, reader);
  }

  /**
   * @param defaultField - The default field for queries with the field name unspecified
   */
  public SpanScorer(Query query, String field,
    CachingTokenFilter cachingTokenFilter, String defaultField) throws IOException {
    this.defaultField = defaultField.intern();
    init(query, field, cachingTokenFilter, null);
  }

  /**
   * @param weightedTerms
   */
  public SpanScorer(WeightedSpanTerm[] weightedTerms) {
    this.fieldWeightedSpanTerms = new HashMap(weightedTerms.length);

    for (int i = 0; i < weightedTerms.length; i++) {
      WeightedSpanTerm existingTerm = (WeightedSpanTerm) fieldWeightedSpanTerms.get(weightedTerms[i].term);

      if ((existingTerm == null) ||
            (existingTerm.weight < weightedTerms[i].weight)) {
        // if a term is defined more than once, always use the highest
        // scoring weight
        fieldWeightedSpanTerms.put(weightedTerms[i].term, weightedTerms[i]);
        maxTermWeight = Math.max(maxTermWeight, weightedTerms[i].getWeight());
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.search.highlight.Scorer#getFragmentScore()
   */
  public float getFragmentScore() {
    return totalScore;
  }

  /**
   *
   * @return The highest weighted term (useful for passing to
   *         GradientFormatter to set top end of coloring scale.
   */
  public float getMaxTermWeight() {
    return maxTermWeight;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.search.highlight.Scorer#getTokenScore(org.apache.lucene.analysis.Token,
   *      int)
   */
  public float getTokenScore(Token token) {
    position += token.getPositionIncrement();
    String termText = token.term();

    WeightedSpanTerm weightedSpanTerm;

    if ((weightedSpanTerm = (WeightedSpanTerm) fieldWeightedSpanTerms.get(
              termText)) == null) {
      return 0;
    }

    if (weightedSpanTerm.positionSensitive &&
          !weightedSpanTerm.checkPosition(position)) {
      return 0;
    }

    float score = weightedSpanTerm.getWeight();

    // found a query term - is it unique in this doc?
    if (!foundTerms.contains(termText)) {
      totalScore += score;
      foundTerms.add(termText);
    }

    return score;
  }

  /**
   * Retrieve the WeightedSpanTerm for the specified token. Useful for passing
   * Span information to a Fragmenter.
   *
   * @param token
   * @return WeightedSpanTerm for token
   */
  public WeightedSpanTerm getWeightedSpanTerm(String token) {
    return (WeightedSpanTerm) fieldWeightedSpanTerms.get(token);
  }

  /**
   * @param query
   * @param field
   * @param tokenStream
   * @param reader
   * @throws IOException
   */
  private void init(Query query, String field,
    CachingTokenFilter cachingTokenFilter, IndexReader reader)
    throws IOException {
    WeightedSpanTermExtractor qse = defaultField == null ? new WeightedSpanTermExtractor()
      : new WeightedSpanTermExtractor(defaultField);
    
    qse.setHighlightCnstScrRngQuery(highlightCnstScrRngQuery);

    if (reader == null) {
      this.fieldWeightedSpanTerms = qse.getWeightedSpanTerms(query,
          cachingTokenFilter, field);
    } else {
      this.fieldWeightedSpanTerms = qse.getWeightedSpanTermsWithScores(query,
          cachingTokenFilter, field, reader);
    }
  }

  /**
   * @return whether ConstantScoreRangeQuerys are set to be highlighted
   */
  public static boolean isHighlightCnstScrRngQuery() {
    return highlightCnstScrRngQuery;
  }

  /**
   * If you call Highlighter#getBestFragment() more than once you must reset
   * the SpanScorer between each call.
   */
  public void reset() {
    position = -1;
  }

  /**
   * Turns highlighting of ConstantScoreRangeQuery on/off. ConstantScoreRangeQuerys cannot be
   * highlighted if you rewrite the query first. Must be called before SpanScorer construction.
   * 
   * @param highlightCnstScrRngQuery
   */
  public static void setHighlightCnstScrRngQuery(boolean highlight) {
    highlightCnstScrRngQuery = highlight;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.search.highlight.Scorer#startFragment(org.apache.lucene.search.highlight.TextFragment)
   */
  public void startFragment(TextFragment newFragment) {
    foundTerms = new HashSet();
    totalScore = 0;
  }
}
