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
package org.apache.solr.common.params;

/**
 *
 * @since solr 1.3
 */
public interface HighlightParams {
  public static final String HIGHLIGHT   = "hl";
  public static final String Q           = HIGHLIGHT+".q";
  public static final String QPARSER     = HIGHLIGHT+".qparser";
  public static final String FIELDS      = HIGHLIGHT+".fl";
  public static final String SNIPPETS    = HIGHLIGHT+".snippets";
  public static final String FRAGSIZE    = HIGHLIGHT+".fragsize";
  public static final String INCREMENT   = HIGHLIGHT+".increment";
  public static final String MAX_CHARS   = HIGHLIGHT+".maxAnalyzedChars";
  public static final String FORMATTER   = HIGHLIGHT+".formatter";
  public static final String ENCODER     = HIGHLIGHT+".encoder";
  public static final String FRAGMENTER  = HIGHLIGHT+".fragmenter";
  public static final String PRESERVE_MULTI = HIGHLIGHT+".preserveMulti";
  public static final String FRAG_LIST_BUILDER = HIGHLIGHT+".fragListBuilder";
  public static final String FRAGMENTS_BUILDER = HIGHLIGHT+".fragmentsBuilder";
  public static final String BOUNDARY_SCANNER = HIGHLIGHT+".boundaryScanner";
  public static final String BS_MAX_SCAN = HIGHLIGHT+".bs.maxScan";
  public static final String BS_CHARS = HIGHLIGHT+".bs.chars";
  public static final String BS_TYPE = HIGHLIGHT+".bs.type";
  public static final String BS_LANGUAGE = HIGHLIGHT+".bs.language";
  public static final String BS_COUNTRY = HIGHLIGHT+".bs.country";
  public static final String BS_VARIANT = HIGHLIGHT+".bs.variant";
  public static final String FIELD_MATCH = HIGHLIGHT+".requireFieldMatch";
  public static final String DEFAULT_SUMMARY = HIGHLIGHT + ".defaultSummary";
  public static final String ALTERNATE_FIELD = HIGHLIGHT+".alternateField";
  public static final String ALTERNATE_FIELD_LENGTH = HIGHLIGHT+".maxAlternateFieldLength";
  public static final String HIGHLIGHT_ALTERNATE = HIGHLIGHT+".highlightAlternate";
  public static final String MAX_MULTIVALUED_TO_EXAMINE = HIGHLIGHT + ".maxMultiValuedToExamine";
  public static final String MAX_MULTIVALUED_TO_MATCH = HIGHLIGHT + ".maxMultiValuedToMatch";
  
  public static final String USE_PHRASE_HIGHLIGHTER = HIGHLIGHT+".usePhraseHighlighter";
  public static final String HIGHLIGHT_MULTI_TERM = HIGHLIGHT+".highlightMultiTerm";
  public static final String PAYLOADS = HIGHLIGHT+".payloads";

  public static final String MERGE_CONTIGUOUS_FRAGMENTS = HIGHLIGHT + ".mergeContiguous";

  public static final String USE_FVH  = HIGHLIGHT + ".useFastVectorHighlighter";
  public static final String TAG_PRE  = HIGHLIGHT + ".tag.pre";
  public static final String TAG_POST = HIGHLIGHT + ".tag.post";
  public static final String TAG_ELLIPSIS = HIGHLIGHT + ".tag.ellipsis";
  public static final String PHRASE_LIMIT = HIGHLIGHT + ".phraseLimit";
  public static final String MULTI_VALUED_SEPARATOR = HIGHLIGHT + ".multiValuedSeparatorChar";
  
  // Formatter
  public static final String SIMPLE = "simple";
  public static final String SIMPLE_PRE  = HIGHLIGHT+"."+SIMPLE+".pre";
  public static final String SIMPLE_POST = HIGHLIGHT+"."+SIMPLE+".post";

  // Regex fragmenter
  public static final String REGEX = "regex";
  public static final String SLOP  = HIGHLIGHT+"."+REGEX+".slop";
  public static final String PATTERN  = HIGHLIGHT+"."+REGEX+".pattern";
  public static final String MAX_RE_CHARS   = HIGHLIGHT+"."+REGEX+".maxAnalyzedChars";
  
  // Scoring parameters
  public static final String SCORE = "score";
  public static final String SCORE_K1 = HIGHLIGHT +"."+SCORE+".k1";
  public static final String SCORE_B = HIGHLIGHT +"."+SCORE+".b";
  public static final String SCORE_PIVOT = HIGHLIGHT +"."+SCORE+".pivot";
}
