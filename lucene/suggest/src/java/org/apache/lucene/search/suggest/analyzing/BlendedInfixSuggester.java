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
package org.apache.lucene.search.suggest.analyzing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

// TODO:
// - allow to use the search score

/**
 * Extension of the AnalyzingInfixSuggester which transforms the weight
 * after search to take into account the position of the searched term into
 * the indexed text.
 * Please note that it increases the number of elements searched and applies the
 * ponderation after. It might be costly for long suggestions.
 *
 * @lucene.experimental
 */
public class BlendedInfixSuggester extends AnalyzingInfixSuggester {

  /**
   * Coefficient used for linear blending
   */
  protected static double LINEAR_COEF = 0.10;

  private Double exponent = 2.0;

  /**
   * Default factor
   */
  public static int DEFAULT_NUM_FACTOR = 10;

  /**
   * Factor to multiply the number of searched elements
   */
  private final int numFactor;

  /**
   * Type of blender used by the suggester
   */
  private final BlenderType blenderType;

  /**
   * The different types of blender.
   */
  public static enum BlenderType {
    /** Application dependent; override {@link
     *  #calculatePositionalCoefficient} to compute it. */
    CUSTOM,
    /** weight*(1 - 0.10*position) */
    POSITION_LINEAR,
    /** weight/(1+position) */
    POSITION_RECIPROCAL,
    /** weight/pow(1+position, exponent) */
    POSITION_EXPONENTIAL_RECIPROCAL
    // TODO:
    //SCORE
  }

  /**
   * Create a new instance, loading from a previously built
   * directory, if it exists.
   */
  public BlendedInfixSuggester(Directory dir, Analyzer analyzer) throws IOException {
    super(dir, analyzer);
    this.blenderType = BlenderType.POSITION_LINEAR;
    this.numFactor = DEFAULT_NUM_FACTOR;
  }

  /**
   * Create a new instance, loading from a previously built
   * directory, if it exists.
   *
   * @param blenderType Type of blending strategy, see BlenderType for more precisions
   * @param numFactor   Factor to multiply the number of searched elements before ponderate
   * @param commitOnBuild Call commit after the index has finished building. This would persist the
   *                      suggester index to disk and future instances of this suggester can use this pre-built dictionary.
   * @throws IOException If there are problems opening the underlying Lucene index.
   */
  public BlendedInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer,
                               int minPrefixChars, BlenderType blenderType, int numFactor, boolean commitOnBuild) throws IOException {
    super(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, commitOnBuild);
    this.blenderType = blenderType;
    this.numFactor = numFactor;
  }

  /**
   * Create a new instance, loading from a previously built
   * directory, if it exists.
   *
   * @param blenderType Type of blending strategy, see BlenderType for more precisions
   * @param numFactor   Factor to multiply the number of searched elements before ponderate
   * @param exponent exponent used only when blenderType is  BlenderType.POSITION_EXPONENTIAL_RECIPROCAL
   * @param commitOnBuild Call commit after the index has finished building. This would persist the
   *                      suggester index to disk and future instances of this suggester can use this pre-built dictionary.
   * @param allTermsRequired All terms in the suggest query must be matched.
   * @param highlight Highlight suggest query in suggestions.
   * @throws IOException If there are problems opening the underlying Lucene index.
   */
  public BlendedInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer,
                               int minPrefixChars, BlenderType blenderType, int numFactor, Double exponent,
                               boolean commitOnBuild, boolean allTermsRequired, boolean highlight) throws IOException {
    super(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, commitOnBuild, allTermsRequired, highlight);
    this.blenderType = blenderType;
    this.numFactor = numFactor;
    if(exponent != null) {
      this.exponent = exponent;
    }
  }

  @Override
  public List<Lookup.LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) throws IOException {
    // Don't * numFactor here since we do it down below, once, in the call chain:
    return super.lookup(key, contexts, onlyMorePopular, num);
  }

  @Override
  public List<Lookup.LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
    // Don't * numFactor here since we do it down below, once, in the call chain:
    return super.lookup(key, contexts, num, allTermsRequired, doHighlight);
  }

  @Override
  public List<Lookup.LookupResult> lookup(CharSequence key, Map<BytesRef, BooleanClause.Occur> contextInfo, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
    // Don't * numFactor here since we do it down below, once, in the call chain:
    return super.lookup(key, contextInfo, num, allTermsRequired, doHighlight);
  }

  @Override
  public List<Lookup.LookupResult> lookup(CharSequence key, BooleanQuery contextQuery, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
    /** We need to do num * numFactor here only because it is the last call in the lookup chain*/
    return super.lookup(key, contextQuery, num * numFactor, allTermsRequired, doHighlight);
  }
  
  @Override
  protected FieldType getTextFieldType() {
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setOmitNorms(true);

    return ft;
  }

  @Override
  protected List<Lookup.LookupResult> createResults(IndexSearcher searcher, TopFieldDocs hits, int num, CharSequence key,
                                                    boolean doHighlight, List<String> matchedTokens, String prefixToken)
      throws IOException {

    TreeSet<Lookup.LookupResult> results = new TreeSet<>(LOOKUP_COMP);

    // we reduce the num to the one initially requested
    int actualNum = num / numFactor;

    for (int i = 0; i < hits.scoreDocs.length; i++) {
      FieldDoc fd = (FieldDoc) hits.scoreDocs[i];

      BinaryDocValues textDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), TEXT_FIELD_NAME);
      assert textDV != null;

      textDV.advance(fd.doc);

      final String text = textDV.binaryValue().utf8ToString();
      long weight = (Long) fd.fields[0];

      // This will just be null if app didn't pass payloads to build():
      // TODO: maybe just stored fields?  they compress...
      BinaryDocValues payloadsDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), "payloads");

      BytesRef payload;
      if (payloadsDV != null) {
        if (payloadsDV.advance(fd.doc) == fd.doc) {
          payload = BytesRef.deepCopyOf(payloadsDV.binaryValue());
        } else {
          payload = new BytesRef(BytesRef.EMPTY_BYTES);
        }
      } else {
        payload = null;
      }

      int matchedTokensCount = matchedTokens.size();
      if (prefixToken != null) {
        matchedTokensCount++;
      }

      double positionalCoefficient = calculatePositionalCoefficient(searcher, fd.doc, matchedTokens, prefixToken, matchedTokensCount);
      double tokenCountCoefficient = getMatchedTokenCountCoefficient(searcher, matchedTokensCount, fd.doc);
      
      if (weight == 0) {
        weight = 1;
      }
      long score = (long) (weight * (97 * positionalCoefficient) + (3 * tokenCountCoefficient));

      LookupResult result;
      if (doHighlight) {
        result = new LookupResult(text, highlight(text, matchedTokens, prefixToken), score, payload);
      } else {
        result = new LookupResult(text, score, payload);
      }

      boundedTreeAdd(results, result, actualNum);
    }

    return new ArrayList<>(results.descendingSet());
  }

  /**
   * Add an element to the tree respecting a size limit
   *
   * @param results the tree to add in
   * @param result the result we try to add
   * @param num size limit
   */
  private static void boundedTreeAdd(TreeSet<Lookup.LookupResult> results, Lookup.LookupResult result, int num) {

    if (results.size() >= num) {
      if (results.first().value < result.value) {
        results.pollFirst();
      } else {
        return;
      }
    }

    results.add(result);
  }

  /**
   * Calculate a coefficient that is affected by the length of the content of the field under evaluation.
   * It measures the intersection of terms between the query terms and the field content.
   * A value of 1 means the field content contains exactly the same number of query terms.
   *
   * @param doc                id of the document
   * @param matchedTokensCount number of tokens found in the query
   * @return the coefficient
   * @throws IOException If there are problems reading term vectors from the underlying Lucene index.
   */
  protected double getMatchedTokenCountCoefficient(IndexSearcher searcher, int matchedTokensCount, int doc) throws IOException {
    Terms tv = searcher.getIndexReader().getTermVector(doc, TEXT_FIELD_NAME);
    double tokenCount = tv.getSumTotalTermFreq();
    return matchedTokensCount / tokenCount;
  }
  
  /**
   * Create the coefficient to transform the weight.
   *
   * @param doc id of the document
   * @param matchedTokens tokens found in the query
   * @param prefixToken unfinished token in the query
   * @return the coefficient
   * @throws IOException If there are problems reading term vectors from the underlying Lucene index.
   */
  protected double calculatePositionalCoefficient(IndexSearcher searcher, int doc, List<String> matchedTokens, String prefixToken, int matchedTokensCount) throws IOException {
    Terms tv = searcher.getIndexReader().getTermVector(doc, TEXT_FIELD_NAME);
    TermsEnum it = tv.iterator();
    BytesRef term;
    int[] matchedTermSuggestionPositions;
    matchedTermSuggestionPositions = new int[matchedTokensCount];
    while ((term = it.next()) != null) {
      String docTerm = term.utf8ToString();
      int matchedTermQueryPosition;
      if ((matchedTermQueryPosition = matchedTokens.indexOf(docTerm)) > -1) {
        int matchedTermSuggestionPosition = getMatchedTermPositionInSuggestion(it, matchedTermQueryPosition);
        matchedTermSuggestionPositions[matchedTermQueryPosition] = matchedTermSuggestionPosition;
      }
      if (prefixToken != null && docTerm.startsWith(prefixToken)) {
        matchedTermQueryPosition = matchedTokensCount - 1;
        int matchedTermSuggestionPosition = getMatchedTermPositionInSuggestion(it, matchedTermQueryPosition);
        matchedTermSuggestionPositions[matchedTermQueryPosition] = matchedTermSuggestionPosition;
      }
    }
    return calculateProductPositionalCoefficient(matchedTermSuggestionPositions);
  }

  private int getMatchedTermPositionInSuggestion(TermsEnum it, int matchedTermQueryPosition) throws IOException {
    PostingsEnum docPosEnum = it.postings(null, PostingsEnum.OFFSETS);
    docPosEnum.nextDoc();
    int matchedTermSuggestionPosition = docPosEnum.nextPosition();
    while (matchedTermSuggestionPosition < matchedTermQueryPosition && matchedTermSuggestionPosition < docPosEnum.freq() - 1) {
      matchedTermSuggestionPosition = docPosEnum.nextPosition();
    }
    return matchedTermSuggestionPosition;
  }

  /**
   * Calculate the weight coefficient based on the position of the first matching word.
   * Subclass should override it to adapt it to particular needs
   * @param position of the first matching word in text
   * @return the coefficient
   */
  protected double calculatePositionalCoefficient(int position) {

    double coefficient;
    switch (blenderType) {
      case POSITION_LINEAR:
        coefficient = 1 - LINEAR_COEF * position;
        break;

      case POSITION_RECIPROCAL:
        coefficient = 1. / (position + 1);
        break;

      case POSITION_EXPONENTIAL_RECIPROCAL:
        coefficient = 1. / Math.pow((position + 1.0), exponent);
        break;

      default:
        coefficient = 1;
    }

    return coefficient;
  }

  /**
   * Calculate the weight coefficient based on the position of all the matching words.
   * It is just the product of the discontinued positions.
   *
   * @param positions of all the matching words in text
   * @return the coefficient
   */
  protected double calculateProductPositionalCoefficient(int[] positions) {
    double coefficient = 1;
    int lastPosition = -1;
    for (int i = 0; i < positions.length; i++) {
      if (positions[i] != lastPosition + 1) {
        double tokenPositionalCoefficient = calculatePositionalCoefficient(positions[i]);
        double idealPositionalCoefficient = calculatePositionalCoefficient(i);
        double normalizedPositionalCoefficient = tokenPositionalCoefficient / idealPositionalCoefficient;// how much the matched token position differs from the ideal position
        double positionalWeightDecay = Math.min(1.0, 1 - calculatePositionalCoefficient(i + 1));// closer the misposition is to the beginning, stronger the decay of the coefficient
        coefficient = coefficient * normalizedPositionalCoefficient * positionalWeightDecay;
      }
      lastPosition = positions[i];
    }
    return coefficient;
  }

  private static Comparator<Lookup.LookupResult> LOOKUP_COMP = new LookUpComparator();

  private static class LookUpComparator implements Comparator<Lookup.LookupResult> {

    @Override
    public int compare(Lookup.LookupResult o1, Lookup.LookupResult o2) {
      // order on weight
      if (o1.value > o2.value) {
        return 1;
      } else if (o1.value < o2.value) {
        return -1;
      }

      // otherwise on alphabetic order
      int keyCompare = CHARSEQUENCE_COMPARATOR.compare(o1.key, o2.key);

      if (keyCompare != 0) {
        return keyCompare;
      }

      // if same weight and title, use the payload if there is one
      if (o1.payload != null) {
        return o1.payload.compareTo(o2.payload);
      }

      return 0;
    }
  }
}

