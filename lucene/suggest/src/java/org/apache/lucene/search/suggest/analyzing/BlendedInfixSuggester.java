package org.apache.lucene.search.suggest.analyzing;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

// TODO:
// - allow to use the search score

/**
 * Extension of the AnalyzingInfixSuggester which transforms the weight
 * after search to take into account the position of the searched term into
 * the indexed text.
 * Please note that it increases the number of elements searched and applies the
 * ponderation after. It might be costly for long suggestions.
 */
public class BlendedInfixSuggester extends AnalyzingInfixSuggester {

  /**
   * Coefficient used for linear blending
   */
  protected static double LINEAR_COEF = 0.10;

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
     *  #calculateCoefficient} to compute it. */
    CUSTOM,
    /** weight*(1 - 0.10*position) */
    POSITION_LINEAR,
    /** weight/(1+position) */
    POSITION_RECIPROCAL,
    // TODO:
    //SCORE
  }

  /**
   * Create a new instance, loading from a previously built
   * directory, if it exists.
   */
  public BlendedInfixSuggester(Version matchVersion, File indexPath, Analyzer analyzer) throws IOException {
    super(matchVersion, indexPath, analyzer);
    this.blenderType = BlenderType.POSITION_LINEAR;
    this.numFactor = DEFAULT_NUM_FACTOR;
  }

  /**
   * Create a new instance, loading from a previously built
   * directory, if it exists.
   *
   * @param blenderType Type of blending strategy, see BlenderType for more precisions
   * @param numFactor   Factor to multiply the number of searched elements before ponderate
   * @throws IOException If there are problems opening the underlying Lucene index.
   */
  public BlendedInfixSuggester(Version matchVersion, File indexPath, Analyzer indexAnalyzer, Analyzer queryAnalyzer,
                               int minPrefixChars, BlenderType blenderType, int numFactor) throws IOException {
    super(matchVersion, indexPath, indexAnalyzer, queryAnalyzer, minPrefixChars);
    this.blenderType = blenderType;
    this.numFactor = numFactor;
  }

  @Override
  public List<Lookup.LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num) {
    // here we multiply the number of searched element by the defined factor
    return super.lookup(key, onlyMorePopular, num * numFactor);
  }

  @Override
  public List<Lookup.LookupResult> lookup(CharSequence key, int num, boolean allTermsRequired, boolean doHighlight) {
    // here we multiply the number of searched element by the defined factor
    return super.lookup(key, num * numFactor, allTermsRequired, doHighlight);
  }

  @Override
  protected FieldType getTextFieldType() {
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setOmitNorms(true);

    return ft;
  }

  @Override
  protected List<Lookup.LookupResult> createResults(TopDocs hits, int num, CharSequence key,
                                                    boolean doHighlight, Set<String> matchedTokens, String prefixToken)
      throws IOException {

    TreeSet<Lookup.LookupResult> results = new TreeSet<Lookup.LookupResult>(LOOKUP_COMP);

    // we reduce the num to the one initially requested
    int actualNum = num / numFactor;

    BytesRef scratch = new BytesRef();
    for (int i = 0; i < hits.scoreDocs.length; i++) {

      ScoreDoc sd = hits.scoreDocs[i];
      textDV.get(sd.doc, scratch);
      String text = scratch.utf8ToString();
      long weight = weightsDV.get(sd.doc);

      BytesRef payload;
      if (payloadsDV != null) {
        payload = new BytesRef();
        payloadsDV.get(sd.doc, payload);
      } else {
        payload = null;
      }

      double coefficient;
      if (text.startsWith(key.toString())) {
        // if hit starts with the key, we don't change the score
        coefficient = 1;
      } else {
        coefficient = createCoefficient(sd.doc, matchedTokens, prefixToken);
      }

      long score = (long) (weight * coefficient);

      LookupResult result;
      if (doHighlight) {
        Object highlightKey = highlight(text, matchedTokens, prefixToken);
        result = new LookupResult(highlightKey.toString(), highlightKey, score, payload);
      } else {
        result = new LookupResult(text, score, payload);
      }

      boundedTreeAdd(results, result, actualNum);
    }

    return new ArrayList<Lookup.LookupResult>(results.descendingSet());
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
   * Create the coefficient to transform the weight.
   *
   * @param doc id of the document
   * @param matchedTokens tokens found in the query
   * @param prefixToken unfinished token in the query
   * @return the coefficient
   * @throws IOException If there are problems reading term vectors from the underlying Lucene index.
   */
  private double createCoefficient(int doc, Set<String> matchedTokens, String prefixToken) throws IOException {

    Terms tv = searcher.getIndexReader().getTermVector(doc, TEXT_FIELD_NAME);
    TermsEnum it = tv.iterator(TermsEnum.EMPTY);

    Integer position = Integer.MAX_VALUE;
    BytesRef term;
    // find the closest token position
    while ((term = it.next()) != null) {

      String docTerm = term.utf8ToString();

      if (matchedTokens.contains(docTerm) || docTerm.startsWith(prefixToken)) {

        DocsAndPositionsEnum docPosEnum = it.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_OFFSETS);
        docPosEnum.nextDoc();

        // use the first occurrence of the term
        int p = docPosEnum.nextPosition();
        if (p < position) {
          position = p;
        }
      }
    }

    // create corresponding coefficient based on position
    return calculateCoefficient(position);
  }

  /**
   * Calculate the weight coefficient based on the position of the first matching word.
   * Subclass should override it to adapt it to particular needs
   * @param position of the first matching word in text
   * @return the coefficient
   */
  protected double calculateCoefficient(int position) {

    double coefficient;
    switch (blenderType) {
      case POSITION_LINEAR:
        coefficient = 1 - LINEAR_COEF * position;
        break;

      case POSITION_RECIPROCAL:
        coefficient = 1. / (position + 1);
        break;

      default:
        coefficient = 1;
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
      return CHARSEQUENCE_COMPARATOR.compare(o1.key, o2.key);
    }
  }

}

