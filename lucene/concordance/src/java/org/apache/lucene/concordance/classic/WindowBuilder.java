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

package org.apache.lucene.concordance.classic;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttributeImpl;
import org.apache.lucene.document.Document;
import org.apache.lucene.concordance.charoffsets.RandomAccessCharOffsetContainer;
import org.apache.lucene.concordance.charoffsets.SimpleAnalyzerUtil;
import org.apache.lucene.concordance.charoffsets.TargetTokenNotFoundException;
import org.apache.lucene.concordance.classic.impl.DefaultSortKeyBuilder;
import org.apache.lucene.concordance.classic.impl.FieldBasedDocIdBuilder;
import org.apache.lucene.concordance.classic.impl.IndexIdDocIdBuilder;
import org.apache.lucene.concordance.classic.impl.SimpleDocMetadataExtractor;


/**
 * Builds a ConcordanceWindow.
 * <p>
 * This class includes basic functionality for building a window from token offsets.
 * <p>
 * It also calls three other components:
 * <ol>
 * <li>DocIdBuilder - extracts or builds a unique key for each document</li>
 * <li>DocMetadataExtractor - extracts metadata from a document to be stored with each window</li>
 * <li>SortKeyBuilder - builds a window's sort key</li>
 * </ol>
 */
public class WindowBuilder {

  private final static String EMPTY_STRING = "";
  private static String INTER_MULTIVALUE_FIELD_PADDING = " | ";
  private final int tokensBefore;
  private final int tokensAfter;
  private final SortKeyBuilder sortKeyBuilder;
  private final DocMetadataExtractor metadataExtractor;
  private final DocIdBuilder docIdBuilder;
  private final int offsetGap;

  public WindowBuilder() {
    this(
        10, //tokens before
        10, //tokens after
        0,
        new DefaultSortKeyBuilder(ConcordanceSortOrder.PRE),
        new SimpleDocMetadataExtractor(),
        new IndexIdDocIdBuilder()
    );
  }

  public WindowBuilder(int tokensBefore, int tokensAfter, int offsetGap) {
    this(
        tokensBefore,
        tokensAfter,
        offsetGap,
        new DefaultSortKeyBuilder(ConcordanceSortOrder.PRE),
        new SimpleDocMetadataExtractor(),
        new IndexIdDocIdBuilder()
    );
  }

  public WindowBuilder(int tokensBefore, int tokensAfter, int offsetGap, SortKeyBuilder sortKeyBuilder,
                       DocMetadataExtractor metadataExtractor, DocIdBuilder docIdBuilder) {
    this.tokensBefore = tokensBefore;
    this.tokensAfter = tokensAfter;
    this.offsetGap = offsetGap;
    this.sortKeyBuilder = sortKeyBuilder;
    this.metadataExtractor = metadataExtractor;
    this.docIdBuilder = docIdBuilder;
  }


  /**
   *
   * Makes the assumption that the target token start and target token end can
   * be found. If not, this returns a null.
   *
   * @param uniqueDocID      ephemeral internal lucene unique document id
   * @param targetTokenStart Target's start token
   * @param targetTokenEnd   Target's end token
   * @param fieldValues      field values
   * @param metadata         Metadata to be stored with the window
   * @param offsets          TokenOffsetResults from
   * @return ConcordanceWindow or null if character offset information cannot be
   * found for both the targetTokenStart and the targetTokenEnd

   * @throws TargetTokenNotFoundException if target token cannot be found
   * @throws IllegalArgumentException if the start token comes after the end token, e.g.
   */
  public ConcordanceWindow buildConcordanceWindow(String uniqueDocID,
                                                  int targetTokenStart, int targetTokenEnd,
                                                  String[] fieldValues,
                                                  RandomAccessCharOffsetContainer offsets,
                                                  Map<String, String> metadata)
      throws TargetTokenNotFoundException,
      IllegalArgumentException {

    if (targetTokenStart < 0 || targetTokenEnd < 0) {
      throw new IllegalArgumentException(
          "targetTokenStart and targetTokenEnd must be >= 0");
    }
    if (targetTokenEnd < targetTokenStart) {
      throw new IllegalArgumentException(
          "targetTokenEnd must be >= targetTokenStart");
    }

    int targetCharStart = offsets.getCharacterOffsetStart(targetTokenStart);
    int targetCharEnd = offsets.getCharacterOffsetEnd(targetTokenEnd);

    if (targetCharStart < 0 ||
        targetCharEnd < 0) {
      throw new TargetTokenNotFoundException(
          "couldn't find character offsets for a target token.\n"
              + "Check that your analyzers are configured properly.\n");
    }

    OffsetAttribute preCharOffset = getPreCharOffset(targetTokenStart,
        targetCharStart, offsets);
    String preString = (preCharOffset == null) ? EMPTY_STRING :
        SimpleAnalyzerUtil.substringFromMultiValuedFields(
            preCharOffset.startOffset(), preCharOffset.endOffset(), fieldValues,
            offsetGap, INTER_MULTIVALUE_FIELD_PADDING);

    OffsetAttribute postCharOffset = getPostCharOffset(targetTokenEnd,
        targetCharEnd, offsets);

    String postString = (postCharOffset == null) ? EMPTY_STRING :
        SimpleAnalyzerUtil.substringFromMultiValuedFields(
            postCharOffset.startOffset(), postCharOffset.endOffset(), fieldValues,
            offsetGap, INTER_MULTIVALUE_FIELD_PADDING);

    String targString = SimpleAnalyzerUtil.substringFromMultiValuedFields(
        targetCharStart, targetCharEnd, fieldValues,
        offsetGap, INTER_MULTIVALUE_FIELD_PADDING);
    ConcordanceSortKey sortKey = sortKeyBuilder.buildKey(uniqueDocID,
        targetTokenStart, targetTokenEnd, offsets, tokensBefore, tokensAfter, metadata);
    int charStart = (preCharOffset == null) ? targetCharStart :
        preCharOffset.startOffset();

    int charEnd = (postCharOffset == null) ? targetCharEnd : postCharOffset.endOffset();
    return new ConcordanceWindow(uniqueDocID, charStart, charEnd, preString, targString,
        postString, sortKey, metadata);

  }


  private OffsetAttribute getPreCharOffset(int targetTokenStart,
                                           int targetCharStart,
                                           RandomAccessCharOffsetContainer charOffsets) {
    if (tokensBefore == 0)
      return null;

    if (targetTokenStart == 0) {
      return null;
    }
    int contextTokenStart = Math.max(0,
        targetTokenStart - tokensBefore);

    int contextCharStart = charOffsets.getClosestCharStart(contextTokenStart, targetTokenStart);
    //closest start wasn't actually found
    //this can happen if there is a large posInc and the target
    //lands at the start of a field index
    if (contextCharStart < 0) {
      return null;
    }
    int contextCharEnd = Math.max(contextCharStart, targetCharStart - 1);

    return buildOffsetAttribute(contextCharStart, contextCharEnd);
  }

  private OffsetAttribute getPostCharOffset(int targetTokenEnd,
                                            int targetCharEnd,
                                            RandomAccessCharOffsetContainer charOffsets) {

    if (tokensAfter == 0)
      return null;

    int contextTokenEnd = targetTokenEnd + tokensAfter;
    int contextCharStart = targetCharEnd;
    int contextCharEnd = charOffsets.getClosestCharEnd(
        contextTokenEnd, targetTokenEnd + 1);

    if (contextCharStart >= contextCharEnd) {
      return null;
    }
    return buildOffsetAttribute(contextCharStart, contextCharEnd);
  }

  private OffsetAttribute buildOffsetAttribute(int start, int end) {
    OffsetAttribute off = new OffsetAttributeImpl();
    off.setOffset(start, end);
    return off;
  }


  public Set<String> getFieldSelector() {
    Set<String> set = new HashSet<>();
    set.addAll(metadataExtractor.getFieldSelector());
    if (docIdBuilder instanceof FieldBasedDocIdBuilder) {
      set.addAll(((FieldBasedDocIdBuilder) docIdBuilder).getFields());
    }
    return set;
  }

  /**
   * Simple wrapper around metadataExtractor
   *
   * @param document document from which to extract metadata
   * @return map
   */
  public Map<String, String> extractMetadata(Document document) {
    return metadataExtractor.extract(document);
  }

  public String getUniqueDocumentId(Document document, long docId) {
    return docIdBuilder.build(document, docId);
  }

  public int getTokensBefore() {
    return tokensBefore;
  }

  public int getTokensAfter() {
    return tokensAfter;
  }
}
