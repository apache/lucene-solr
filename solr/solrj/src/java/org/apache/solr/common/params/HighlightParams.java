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
  // primary
  public static final String HIGHLIGHT   = "hl";
  public static final String METHOD      = HIGHLIGHT+".method"; // original|fastVector|postings|unified
  public static final String FIELDS      = HIGHLIGHT+".fl";
  public static final String SNIPPETS    = HIGHLIGHT+".snippets";

  //    KEY:
  // OH = (original) Highlighter   (AKA the standard Highlighter)
  // FVH = FastVectorHighlighter
  // UH = UnifiedHighlighter (evolved from PostingsHighlighter)

  // query interpretation
  public static final String Q           = HIGHLIGHT+".q"; // all
  public static final String QPARSER     = HIGHLIGHT+".qparser"; // all
  public static final String FIELD_MATCH = HIGHLIGHT+".requireFieldMatch"; // OH, FVH, UH
  public static final String USE_PHRASE_HIGHLIGHTER = HIGHLIGHT+".usePhraseHighlighter"; // OH, FVH, UH
  public static final String HIGHLIGHT_MULTI_TERM = HIGHLIGHT+".highlightMultiTerm"; // all

  // if no snippets...
  public static final String DEFAULT_SUMMARY = HIGHLIGHT + ".defaultSummary"; // UH
  public static final String ALTERNATE_FIELD = HIGHLIGHT+".alternateField"; // OH, FVH
  public static final String ALTERNATE_FIELD_LENGTH = HIGHLIGHT+".maxAlternateFieldLength"; // OH, FVH
  public static final String HIGHLIGHT_ALTERNATE = HIGHLIGHT+".highlightAlternate"; // OH, FVH

  // sizing
  public static final String FRAGSIZE    = HIGHLIGHT+".fragsize"; // OH, FVH, UH
  public static final String FRAGSIZEISMINIMUM = HIGHLIGHT+".fragsizeIsMinimum"; // UH
  public static final String FRAGALIGNRATIO = HIGHLIGHT+".fragAlignRatio"; // UH
  public static final String FRAGMENTER  = HIGHLIGHT+".fragmenter"; // OH
  public static final String INCREMENT   = HIGHLIGHT+".increment"; // OH
  public static final String REGEX       = "regex"; // OH
  public static final String SLOP        = HIGHLIGHT+"."+REGEX+".slop"; // OH
  public static final String PATTERN     = HIGHLIGHT+"."+REGEX+".pattern"; // OH
  public static final String MAX_RE_CHARS= HIGHLIGHT+"."+REGEX+".maxAnalyzedChars"; // OH
  public static final String BOUNDARY_SCANNER = HIGHLIGHT+".boundaryScanner"; // FVH
  public static final String BS_MAX_SCAN = HIGHLIGHT+".bs.maxScan"; // FVH
  public static final String BS_CHARS    = HIGHLIGHT+".bs.chars"; // FVH
  public static final String BS_TYPE     = HIGHLIGHT+".bs.type"; // FVH, UH
  public static final String BS_LANGUAGE = HIGHLIGHT+".bs.language"; // FVH, UH
  public static final String BS_COUNTRY  = HIGHLIGHT+".bs.country"; // FVH, UH
  public static final String BS_VARIANT  = HIGHLIGHT+".bs.variant"; // FVH, UH
  public static final String BS_SEP      = HIGHLIGHT+".bs.separator"; // UH

  // formatting
  public static final String FORMATTER   = HIGHLIGHT+".formatter"; // OH
  public static final String ENCODER     = HIGHLIGHT+".encoder"; // all
  public static final String MERGE_CONTIGUOUS_FRAGMENTS = HIGHLIGHT + ".mergeContiguous"; // OH
  public static final String SIMPLE      = "simple"; // OH
  public static final String SIMPLE_PRE  = HIGHLIGHT+"."+SIMPLE+".pre"; // OH
  public static final String SIMPLE_POST = HIGHLIGHT+"."+SIMPLE+".post"; // OH
  public static final String FRAGMENTS_BUILDER = HIGHLIGHT+".fragmentsBuilder"; // FVH
  public static final String TAG_PRE     = HIGHLIGHT + ".tag.pre"; // FVH, UH
  public static final String TAG_POST    = HIGHLIGHT + ".tag.post"; // FVH, UH
  public static final String TAG_ELLIPSIS= HIGHLIGHT + ".tag.ellipsis"; // FVH, UH
  public static final String MULTI_VALUED_SEPARATOR = HIGHLIGHT + ".multiValuedSeparatorChar"; // FVH

  // ordering
  public static final String PRESERVE_MULTI = HIGHLIGHT+".preserveMulti"; // OH
  public static final String FRAG_LIST_BUILDER = HIGHLIGHT+".fragListBuilder"; // FVH
  public static final String SCORE = "score"; // UH
  public static final String SCORE_K1 = HIGHLIGHT +"."+SCORE+".k1"; // UH
  public static final String SCORE_B = HIGHLIGHT +"."+SCORE+".b"; // UH
  public static final String SCORE_PIVOT = HIGHLIGHT +"."+SCORE+".pivot"; // UH

  // misc
  public static final String MAX_CHARS   = HIGHLIGHT+".maxAnalyzedChars"; // all
  public static final String PAYLOADS = HIGHLIGHT+".payloads"; // OH
  public static final String MAX_MULTIVALUED_TO_EXAMINE = HIGHLIGHT + ".maxMultiValuedToExamine"; // OH
  public static final String MAX_MULTIVALUED_TO_MATCH = HIGHLIGHT + ".maxMultiValuedToMatch"; // OH
  public static final String PHRASE_LIMIT = HIGHLIGHT + ".phraseLimit"; // FVH
  public static final String OFFSET_SOURCE = HIGHLIGHT + ".offsetSource"; // UH
  public static final String CACHE_FIELD_VAL_CHARS_THRESHOLD = HIGHLIGHT + ".cacheFieldValCharsThreshold"; // UH
  public static final String WEIGHT_MATCHES = HIGHLIGHT + ".weightMatches"; // UH
}
