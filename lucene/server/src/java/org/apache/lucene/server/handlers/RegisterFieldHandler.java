package org.apache.lucene.server.handlers;

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
import java.io.Reader;
import java.io.StringReader;
import java.text.Collator;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.icu.segmentation.DefaultICUTokenizerConfig;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizerConfig;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.collation.CollationKeyAnalyzer;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.suggest.analyzing.SuggestStopFilter;
import org.apache.lucene.server.FieldDef;
import org.apache.lucene.server.FieldDefBindings;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import org.apache.lucene.server.params.PolyType.PolyEntry;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;

/** Handles {@code registerFields}. */
public class RegisterFieldHandler extends Handler {

  private final static List<Object> DEFAULT_ENGLISH_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : EnglishAnalyzer.getDefaultStopSet()) {
      DEFAULT_ENGLISH_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_ARABIC_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : ArabicAnalyzer.getDefaultStopSet()) {
      DEFAULT_ARABIC_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_ARMENIAN_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : ArmenianAnalyzer.getDefaultStopSet()) {
      DEFAULT_ARMENIAN_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_BASQUE_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : BasqueAnalyzer.getDefaultStopSet()) {
      DEFAULT_BASQUE_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_BRAZILIAN_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : BrazilianAnalyzer.getDefaultStopSet()) {
      DEFAULT_BRAZILIAN_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_BULGARIAN_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : BulgarianAnalyzer.getDefaultStopSet()) {
      DEFAULT_BULGARIAN_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_CJK_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : CJKAnalyzer.getDefaultStopSet()) {
      DEFAULT_CJK_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_CATALAN_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : CatalanAnalyzer.getDefaultStopSet()) {
      DEFAULT_CATALAN_STOP_WORDS.add(new String((char[]) o));
    }
  }

  private final static List<Object> DEFAULT_GERMAN_STOP_WORDS = new ArrayList<Object>();
  static {
    for(Object o : GermanAnalyzer.getDefaultStopSet()) {
      DEFAULT_GERMAN_STOP_WORDS.add(new String((char[]) o));
    }
  }

  // Breaks the recursion:
  private final static WrapType ANALYZER_TYPE_WRAP = new WrapType();

  /** Type to accept lucene versions. */
  public final static Param MATCH_VERSION_PARAM = new Param("matchVersion", "Lucene version to match.", new EnumType("LUCENE_40", "LUCENE_40",
                                                                                                                     "LUCENE_41", "LUCENE_41",
                                                                                                                     "LUCENE_42", "LUCENE_42",
                                                                                                                     "LUCENE_43", "LUCENE_43",
                                                                                                                     "LUCENE_44", "LUCENE_44",
                                                                                                                     "LUCENE_45", "LUCENE_45",
                                                                                                                     "LUCENE_46", "LUCENE_46"));

  /** Analyzer type. */
  final static Type ANALYZER_TYPE =
    new StructType(
                   // nocommit cutover to PolyType
                   new Param("class",
                             "An existing Analyzer class.  Use either this, or define your own analysis chain by setting tokenizer and tokenFilter.",
                             new PolyType(Analyzer.class,
                                          new PolyEntry("ArabicAnalyzer", "Analyzer for Arabic (see @lucene:analyzers-common:org.apache.lucene.analysis.ar.ArabicAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_ARABIC_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("ArmenianAnalyzer", "Analyzer for Armenian. (see @lucene:analyzers-common:org.apache.lucene.analysis.hy.ArmenianAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_ARMENIAN_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("BasqueAnalyzer", "Analyzer for Basque. (see @lucene:analyzers-common:org.apache.lucene.analysis.eu.BasqueAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_BASQUE_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("BrazilianAnalyzer", "Analyzer for Brazilian Portuguese language (see @lucene:analyzers-common:org.apache.lucene.analysis.br.BrazilianAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_BRAZILIAN_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("BulgarianAnalyzer", "Analyzer for Bulgarian (see @lucene:analyzers-common:org.apache.lucene.analysis.bg.BulgarianAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_BULGARIAN_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("CatalanAnalyzer", "Analyzer for Catalan (see @lucene:analyzers-common:org.apache.lucene.analysis.ca.CatalanAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_BULGARIAN_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("CJKAnalyzer", "An Analyzer that tokenizes text with StandardTokenizer, normalizes content with CJKWidthFilter, folds case with LowerCaseFilter, forms bigrams of CJK with CJKBigramFilter, and filters stopwords with StopFilter (see @lucene:analyzers-common:org.apache.lucene.analysis.cjk.CJKAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis",
                                                                  new ListType(new StringType()), DEFAULT_CJK_STOP_WORDS)),
                                          new PolyEntry("CollationKeyAnalyzer", "<p> Configures KeywordTokenizer with CollationAttributeFactory (see @lucene:analyzers-common:org.apache.lucene.collation.CollationKeyAnalyzer)",
                                                        new Param("locale", "Locale", SearchHandler.LOCALE_TYPE)),
                                          new PolyEntry("EnglishAnalyzer", "Analyzer for English. (see @lucene:analyzers-common:org.apache.lucene.analysis.en.EnglishAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis.",
                                                                  new ListType(new StringType()), DEFAULT_ENGLISH_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("GermanAnalyzer", "Analyzer for German language (see @lucene:analyzers-common:org.apache.lucene.analysis.de.GermanAnalyzer)",
                                                        new Param("stopWords", "Stop words to remove during analysis.",
                                                                  new ListType(new StringType()), DEFAULT_GERMAN_STOP_WORDS),
                                                        new Param("stemExclusionSet", "A set of terms not to be stemmed",
                                                                  new ListType(new StringType()))),
                                          new PolyEntry("StandardAnalyzer", "Filters StandardTokenizer with StandardFilter, LowerCaseFilter and StopFilter, using a list of English stop words (see @lucene:analyzers-common:org.apache.lucene.analysis.standard.StandardAnalyzer)",
                                                        new Param("maxTokenLength", "Max token length.", new IntType(), StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH),
                                                        new Param("stopWords", "Stop words to remove during analysis.",
                                                                  new ListType(new StringType()), DEFAULT_ENGLISH_STOP_WORDS)),
                                          new PolyEntry("WhitespaceAnalyzer", "An Analyzer that uses WhitespaceTokenizer (see @lucene:analyzers-common:org.apache.lucene.analysis.core.WhitespaceAnalyzer)")),
                             "StandardAnalyzer"),
                   new Param("positionIncrementGap", "How many positions to insert between separate values in a multi-valued field", new IntType(), 0),
                   new Param("offsetGap", "How many offsets to insert between separate values in a multi-valued field", new IntType(), 1),
                   new Param("tokenizer", "Tokenizer class (for a custom analysis chain).",
                             new StructType(
                                 new Param("class",
                                           "Tokenizer class",
                                           new PolyType(Tokenizer.class,
                                                        new PolyEntry("WhitespaceTokenizer", "A WhitespaceTokenizer is a tokenizer that divides text at whitespace (see @lucene:analyzers-common:org.apache.lucene.analysis.core.WhitespaceTokenizer)", new StructType()),
                                                        new PolyEntry("StandardTokenizer", "A grammar-based tokenizer constructed with JFlex (see @lucene:analyzers-common:org.apache.lucene.analysis.standard.StandardTokenizer)",
                                                            new Param("maxTokenLength", "Max length of each token", new IntType(), StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH)),
                                                        new PolyEntry("PatternTokenizer", "This tokenizer uses regex pattern matching to construct distinct tokens for the input stream (see @lucene:analyzers-common:org.apache.lucene.analysis.pattern.PatternTokenizer)",
                                                            new Param("pattern", "Regular expression pattern", new StringType()),
                                                                      new Param("group", "Group index for the tokens (-1 to do 'split')", new IntType(), -1)),
                                                        new PolyEntry("ICUTokenizer", "Breaks text into words according to UAX #29: Unicode Text Segmentation (http://www.unicode.org/reports/tr29",
                                                            new Param("rules", "Customize the tokenization per-script",
                                                               new ListType(
                                                                   new StructType(
                                                                       new Param("script", "Script", new StringType()),
                                                                       new Param("rules", "Rules", new StringType()))))))))),
                   new Param("tokenFilters", "Optional list of TokenFilters to apply after the Tokenizer",
                             new ListType(
                                 new StructType(
                                     new Param("class", "TokenFilter class",
                                         new PolyType(TokenFilter.class,
                                                      new PolyEntry("ArabicStemFilter", "A TokenFilter that applies ArabicStemmer to stem Arabic words. (see @lucene:analyzers-common:org.apache.lucene.analysis.ar.ArabicStemFilter)"),
                                                      new PolyEntry("StandardFilter", "Normalizes tokens extracted with StandardTokenizer. (see @lucene:analyzers-common:org.apache.lucene.analysis.standard.StandardFilter)"),
                                                      new PolyEntry("EnglishPossessiveFilter", "TokenFilter that removes possessives (trailing 's) from words (see @lucene:analyzers-common:org.apache.lucene.analysis.en.EnglishPossessiveFilter)"),
                                                      new PolyEntry("PorterStemFilter", "Transforms the token stream as per the Porter stemming algorithm (see @lucene:analyzers-common:org.apache.lucene.analysis.en.PorterStemFilter)"),
                                                      new PolyEntry("SuggestStopFilter", "Like {@link StopFilter} except it will not remove the last token if that token was not followed by some token separator.",
                                                                    new Param("stopWords", "Stop words to remove during analysis",
                                                                        new ListType(new StringType()), DEFAULT_ENGLISH_STOP_WORDS)),
                                                      new PolyEntry("EnglishMinimalStemFilter", "A TokenFilter that applies EnglishMinimalStemmer to stem English words (see @lucene:analyzers-common:org.apache.lucene.analysis.en.EnglishMinimalStemFilter)"),
                                                      new PolyEntry("SetKeywordMarkerFilter", "Marks terms as keywords via the KeywordAttribute (see @lucene:analyzers-common:org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter)", new Param("keyWords", "List of tokens to mark as keywords", new ListType(new StringType()))),
                                                      new PolyEntry("StopFilter", "Removes stop words from a token stream (see @lucene:analyzers-common:org.apache.lucene.analysis.core.StopFilter)",
                                                          new Param("stopWords", "Stop words to remove during analysis",
                                                                    new ListType(new StringType()), DEFAULT_ENGLISH_STOP_WORDS)),
                                                      new PolyEntry("SynonymFilter", "Matches single- or multi-word synonyms and injects or replaces the match with a corresponding synonym",
                                                          //new Param("synonymFile", "Local file to load synonyms from", new StringType()),   // nocommit TODO
                                                          new Param("ignoreCase", "True if matching should be case insensitive", new BooleanType(), true),
                                                          new Param("analyzer", "Analyzer to use to tokenize synonym inputs", ANALYZER_TYPE_WRAP),
                                                          new Param("synonyms", "Specify synonyms inline (instead of synonymFile)",
                                                              new ListType(
                                                                  new StructType(
                                                                      new Param("input", "String or list of strings with input token(s) to match", new OrType(new ListType(new StringType()), new StringType())),
                                                                      // TODO: allow more than one token on the output?
                                                                      new Param("output", "Single token to replace the matched tokens with", new StringType()),
                                                                      new Param("replace", "True if the input tokens should be replaced with the output token; false if the input tokens should be preserved and the output token overlaid", new BooleanType(), true))))),
                                                      new PolyEntry("LowerCaseFilter", "Normalizes token text to lower case (see @lucene:analyzers-common:org.apache.lucene.analysis.core.LowerCaseFilter)", new StructType())))))),
                   MATCH_VERSION_PARAM);

  static {
    ANALYZER_TYPE_WRAP.set(ANALYZER_TYPE);
  }

  // nocommit need not be separate TYPE anymore!
  private static final StructType BM25_SIM_TYPE =
    new StructType(new Param("k1", "Controls non-linear term frequency normalization (saturation).", new FloatType(), 1.2f),
                   new Param("b", "Controls to what degree document length normalizes tf values.", new FloatType(), 0.75f));

  // nocommit need not be separate TYPE anymore!
  private static final StructType DEFAULT_SIM_TYPE = new StructType();

  private final static StructType FIELD_TYPE =
    new StructType(
        new Param("type", "Type of the value.",
                  new EnumType("text", "Text that's tokenized and indexed, with the index-time analyzer.",
                               "atom", "Text that's indexed as a single token, with DOCS_ONLY and omitting norms.",
                               "boolean", "Boolean value.",
                               "float", "Float value.",
                               "double", "Double value.",
                               "int", "Int value.",
                               "long", "Long value.",
                               // TODO: this is hacked up now ... only supports fixed "recency" blending ... ideally we would accept
                               // a custom equation and parse & execute that:
                               // nocommit name this "dynamic" instead of "virtual"?
                               "virtual", "Virtual (computed at search time) field, e.g. for blended sorting.")),
        // nocommit rename to "search"?  ie, "I will search on/by this field's values"
        new Param("index", "True if the value should be indexed.", new BooleanType(), false),
        new Param("tokenize", "True if the value should be tokenized.", new BooleanType(), true),
        new Param("store", "True if the value should be stored.", new BooleanType(), false),
        new Param("multiValued", "True if this field may sometimes have more than one value.", new BooleanType(), false),
        new Param("highlight", "True if the value should be indexed for highlighting.", new BooleanType(), false),
        new Param("postingsFormat", "Which PostingsFormat should be used to index this field.",
                  new StringType(), "Lucene41"),
        new Param("docValuesFormat", "Which DocValuesFormat should be used to index this field.",
                  new StringType(), "Lucene45"),
        new Param("sort", "True if the value should be indexed into doc values for sorting.", new BooleanType(), false),
        new Param("group", "True if the value should be indexed into doc values for grouping.", new BooleanType(), false),
        new Param("facet", "Whether this field should index facets, and how.",
                  new EnumType("no", "No facets are indexed.",
                               "flat", "Facets are indexed with no hierarchy.",
                               "hierarchy", "Facets are indexed and are hierarchical.",
                               "numericRange", "Compute facet counts for custom numeric ranges",
                               "sortedSetDocValues", "Use SortedSetDocValuesFacetCounts, which must be flat but don't require a taxonomy index"),
                  "no"),
        new Param("storeDocValues", "Whether to index the value into doc values.", new BooleanType(), false),
        new Param("liveValues", "Enable live values for this field: whenever this field is retrieved during a search, the live (most recetly added) value will always be returned; set this to the field name of your id (primary key) field.  Uses @lucene:core:org.apache.lucene.index.LiveFieldValues under the hood.", new StringType()),
        new Param("numericPrecisionStep", "If the value is numeric, what precision step to use during indexing.", new IntType(), NumericUtils.PRECISION_STEP_DEFAULT),
        new Param("omitNorms", "True if norms are omitted.", new BooleanType(), false),
        new Param("analyzer", "Analyzer to use for this field during indexing and searching.", ANALYZER_TYPE),
        new Param("indexAnalyzer", "Analyzer to use for this field during indexing.", ANALYZER_TYPE),
        new Param("searchAnalyzer", "Analyzer to use for this field during searching.", ANALYZER_TYPE),
        new Param("indexOptions", "How the tokens should be indexed.",
                  new EnumType("docs", "Index only doc ids (for binary search).",
                               "docsFreqs", "Index doc ids and term frequencies.",
                               "docsFreqsPositions", "Index doc ids, term frequences and positions.",
                               "docsFreqsPositionsOffsets", "Index doc ids, term frequencies, positions and offsets."),
                  "docsFreqsPositions"),
        new Param("expression", "The JavaScript expression defining a virtual field's value (only used with type=virtual).", new StringType()),
        new Param("recencyScoreBlend", "Only used with type=virtual, to describe how the virtual field blends with score.",
                  new StructType(
                                 new Param("timeStampField", "Field holding timestamp value (must be type long, with sort=true)", new StringType()),
                                 new Param("maxBoost", "Maximum boost to apply to the relevance score (for the most recent matches)", new FloatType()),
                                 new Param("range", "Age beyond which no boosting occurs", new LongType()))),
        new Param("termVectors", "Whether/how term vectors should be indexed.",
                  new EnumType("terms", "Index terms and freqs only.",
                               "termsPositions", "Index terms, freqs and positions.",
                               "termsPpositionsOffsets", "Index terms, freqs, positions and offsets.",
                               "termsPositionsdOoffsetsPayloads", "Index terms, freqs, positions, offsets and payloads."
                               )),
        new Param("similarity", "Which Similarity implementation to use for this field.",
                  new StructType(
                                 new Param("class",
                                           "Which Similarity class to use.",
                                           new PolyType(Similarity.class,
                                               new PolyEntry("DefaultSimilarity", "Expert: Default scoring implementation. (see @lucene:core:org.apache.lucene.search.similarities.DefaultSimilarity)", DEFAULT_SIM_TYPE),
                                               new PolyEntry("BM25Similarity", "BM25 Similarity (see @lucene:core:org.apache.lucene.search.similarities.BM25Similarity)", BM25_SIM_TYPE)),
                                           "DefaultSimilarity")))
                   );

  /** Method type. */
  public final static StructType TYPE =
    new StructType(
        new Param("indexName", "Index name", new StringType()),
        new Param("fields", "New fields to register",
            new StructType(new Param("*", "Register this field name with the provided type.  Note that the field name must be of the form [a-zA-Z_][a-zA-Z_0-9]*.  You can register multiple fields in one request.", FIELD_TYPE))));

  /** Sole constructor. */
  public RegisterFieldHandler(GlobalState state) {
    super(state);
  }

  @Override
  public String getTopDoc() {
    return "Registers one or more fields.  Fields must be registered before they can be added in a document (via @addDocument).  Pass a struct whose keys are the fields names to register and whose values define the type for that field.  Any number of fields may be registered in a single request, and once a field is registered it cannot be changed (write-once).  <p>This returns the full set of fields currently registered.";
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  private FieldDef parseOneVirtualFieldType(Request r, IndexState state, Map<String,FieldDef> pendingFieldDefs, String name, JSONObject o) {
    if (r.hasParam("expression")) {
      String exprString = r.getString("expression");
      Expression expr;

      try {
        expr = JavascriptCompiler.compile(exprString);
      } catch (ParseException pe) {
        // Static error (e.g. bad JavaScript syntax):
        r.fail("expression", "could not parse expression: " + pe, pe);

        // Dead code but compiler disagrees:
        expr = null;
      } catch (IllegalArgumentException iae) {
        // Static error (e.g. bad JavaScript syntax):
        r.fail("expression", "could not parse expression: " + iae, iae);

        // Dead code but compiler disagrees:
        expr = null;
      }

      Map<String,FieldDef> allFields = new HashMap<String,FieldDef>(state.getAllFields());
      allFields.putAll(pendingFieldDefs);

      ValueSource values;
      try {
        values = expr.getValueSource(new FieldDefBindings(allFields));
      } catch (RuntimeException re) {
        // Dynamic error (e.g. referred to a field that
        // doesn't exist):
        r.fail("expression", "could not evaluate expression: " + re, re);

        // Dead code but compiler disagrees:
        values = null;
      }

      return new FieldDef(name, null, "virtual", null, null, null, true, null, null, null, false, null, null, 0.0f, 0L, values);

    } else {
      // nocommit cutover all tests to expression fields and remove this hack:
      Request r2 = r.getStruct("recencyScoreBlend");
      String timeStampField = r2.getString("timeStampField");
      FieldDef fd;
      try {
        fd = state.getField(timeStampField);
      } catch (IllegalArgumentException iae) {
        fd = pendingFieldDefs.get(timeStampField);
        if (fd == null) {
          r2.fail("timeStampField", "field \"" + timeStampField + "\" was not yet registered");
        }
      }
      if (fd.fieldType.docValueType() != DocValuesType.NUMERIC) {
        r2.fail("timeStampField", "field \"" + fd.name + "\" must be registered with type=long and sort=true");
      }
      float maxBoost = r2.getFloat("maxBoost");
      long range = r2.getLong("range");
      return new FieldDef(name, null, "virtual", null, null, null, true, null, null, null, false, null, fd.name, maxBoost, range, null);
    }
  }

  private FieldDef parseOneFieldType(Request r, IndexState state, Map<String,FieldDef> pendingFieldDefs, String name, JSONObject o) {

    // This way f.fail reports which field name was problematic:
    Request f = new Request(r, name, o, FIELD_TYPE);

    String type = f.getEnum("type");
    if (type.equals("virtual")) {
      return parseOneVirtualFieldType(f, state, pendingFieldDefs, name, o);
    }

    FieldType ft = new FieldType();

    boolean dv = f.getBoolean("storeDocValues");
    boolean sorted = f.getBoolean("sort");
    boolean grouped = f.getBoolean("group");

    // nocommit: user must say which highlighter?  ie, we
    // may index offsets into postings, or term vectors...
    boolean highlighted = f.getBoolean("highlight");

    if (highlighted) {
      if (type.equals("text") == false && type.equals("atom") == false) {
        f.fail("highlighted", "only type=text or type=atom fields can be highlighted");
      }
    }

    boolean multiValued = f.getBoolean("multiValued");
    if (multiValued) {
      if (sorted) {
        f.fail("multiValued", "cannot sort on multiValued fields");
      }
      if (grouped) {
        f.fail("multiValued", "cannot group on multiValued fields");
      }
    }      

    if (type.equals("text")) {
      ft.setIndexed(true);
      ft.setTokenized(true);
      if (sorted || grouped) {
        ft.setDocValueType(DocValuesType.SORTED);
      } else if (dv) {
        ft.setDocValueType(DocValuesType.BINARY);
      }
      if (highlighted) {
        ft.setStored(true);
        ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      }
    } else if (type.equals("atom")) {
      if (f.hasParam("analyzer")) {
        f.fail("analyzer", "no analyzer allowed with atom (it's hardwired to KeywordAnalyzer internally)");
      }
      ft.setIndexed(true);
      ft.setIndexOptions(IndexOptions.DOCS_ONLY);
      ft.setOmitNorms(true);
      ft.setTokenized(false);
      if (sorted || grouped) {
        ft.setDocValueType(DocValuesType.SORTED);
      } else if (grouped || dv) {
        ft.setDocValueType(DocValuesType.BINARY);
      }
    } else if (type.equals("boolean")) {
      if (dv || sorted || grouped) {
        ft.setDocValueType(DocValuesType.NUMERIC);
      }
    } else if (type.equals("long")) {
      if (dv || sorted || grouped) {
        ft.setDocValueType(DocValuesType.NUMERIC);
      }
    } else if (type.equals("int")) {
      if (dv || sorted || grouped) {
        ft.setDocValueType(DocValuesType.NUMERIC);
      }
    } else if (type.equals("double")) {
      if (dv || sorted || grouped) {
        ft.setDocValueType(DocValuesType.NUMERIC);
      }
    } else if (type.equals("float")) {
      if (dv || sorted || grouped) {
        ft.setDocValueType(DocValuesType.NUMERIC);
      }
    } else {
      assert false;
    }

    if (f.hasParam("store")) {
      ft.setStored(f.getBoolean("store"));
      if (!ft.stored() && highlighted) {
        f.fail("store", "store cannot be False when highlighted is True");
      }
    }

    if (f.hasParam("index")) {
      ft.setIndexed(f.getBoolean("index"));
    }

    if (f.hasParam("analyzer") && !ft.indexed()) {
      f.fail("analyzer", "no analyzer allowed when indexed is false");
    }

    // nocommit hierarchical facet field cannot be indexed &
    // stored; make test & check that here

    // nocommit make sure a useless field (not indexed,
    // stored, dv'd nor faceted) is detected!

    if (type.equals("text") || type.equals("atom")) {

      if (ft.indexed()) {
        if (f.hasParam("tokenize")) {
          ft.setTokenized(f.getBoolean("tokenize"));
        }
        if (f.hasParam("omitNorms")) {
          ft.setOmitNorms(f.getBoolean("omitNorms"));
        }

        if (f.hasParam("termVectors")) {
          String tv = f.getString("termVectors");
          if (tv.equals("terms")) {
            ft.setStoreTermVectors(true);
          } else if (tv.equals("termsPositions")) {
            ft.setStoreTermVectors(true);
            ft.setStoreTermVectorPositions(true);
          } else if (tv.equals("termsPositionsOffsets")) {
            ft.setStoreTermVectors(true);
            ft.setStoreTermVectorPositions(true);
            ft.setStoreTermVectorOffsets(true);
          } else if (tv.equals("termsPositionsOffsetsPayloads")) {
            ft.setStoreTermVectors(true);
            ft.setStoreTermVectorPositions(true);
            ft.setStoreTermVectorOffsets(true);
            ft.setStoreTermVectorPayloads(true);
          } else {
            assert false;
          }
        }

        if (f.hasParam("indexOptions")) {
          String io = f.getString("indexOptions");
          if (io.equals("docs")) {
            ft.setIndexOptions(IndexOptions.DOCS_ONLY);
          } else if (io.equals("docsFreqs")) {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
          } else if (io.equals("docsFreqsPositions")) {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
          } else if (io.equals("docsFreqsPositionsAndOffsets")) {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
          } else {
            assert false;
          }
        }
      }
    } else if (type.equals("boolean")) {
      ft.setIndexed(true);
      ft.setOmitNorms(true);
      ft.setTokenized(false);
      ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    } else if (ft.indexed()) {
      ft.setNumericPrecisionStep(f.getInt("numericPrecisionStep"));
      if (type.equals("float")) {
        ft.setNumericType(NumericType.FLOAT);
      } else if (type.equals("double")) {
        ft.setNumericType(NumericType.DOUBLE);
      } else if (type.equals("long")) {
        ft.setNumericType(NumericType.LONG);
      } else {
        assert type.equals("int"): "type=" + type;
        ft.setNumericType(NumericType.INT);
      }
    }

    String pf = f.getString("postingsFormat");
    if (PostingsFormat.forName(pf) == null) {
      f.fail("postingsFormat", "unrecognized postingsFormat \"" + pf + "\"");
    }
    String dvf = f.getString("docValuesFormat");
    if (DocValuesFormat.forName(dvf) == null) {
      f.fail("docValuesFormat", "unrecognized docValuesFormat \"" + dvf + "\"");
    }

    Similarity sim;
    if (f.hasParam("similarity")) {
      Request s = f.getStruct("similarity");
      Request.PolyResult pr = s.getPoly("class");
      if (pr.name.equals("DefaultSimilarity")) {
        sim = new DefaultSimilarity();
      } else if (pr.name.equals("BM25Similarity")) {
        sim = new BM25Similarity(pr.r.getFloat("k1"), pr.r.getFloat("b"));
      } else {
        assert false;
        sim = null;
      }
    } else {
      sim = new DefaultSimilarity();
    }

    Analyzer indexAnalyzer;
    Analyzer searchAnalyzer;
    Analyzer analyzer = getAnalyzer(state.matchVersion, f, "analyzer");
    if (analyzer != null) {
      indexAnalyzer = searchAnalyzer = analyzer;
    } else {
      indexAnalyzer = getAnalyzer(state.matchVersion, f, "indexAnalyzer");
      searchAnalyzer = getAnalyzer(state.matchVersion, f, "searchAnalyzer");
    }

    if (type.equals("text") && ft.indexed()) {
      if (indexAnalyzer == null) {
        f.fail("indexAnalyzer", "field=\"" + name + "\": either analyzer or indexAnalyzer must be specified for an indexed text field");
      }
      if (searchAnalyzer == null) {
        f.fail("searchAnalyzer", "field=\"" + name + "\": either analyzer or searchAnalyzer must be specified for an indexed text field");
      }
    }

    if (indexAnalyzer == null) {
      indexAnalyzer = dummyAnalyzer;
    }

    if (searchAnalyzer == null) {
      searchAnalyzer = searchAnalyzer;
    }

    String liveValuesIDField;
    if (f.hasParam("liveValues")) {
      // nocommit: sort of silly you cannot register id & live
      // fields in same request...
      FieldDef idField = state.getField(f, "liveValues");
      liveValuesIDField = idField.name;
      if (!type.equals("atom") && !type.equals("atom")) {
        f.fail("liveValues", "only type=atom or type=text fields may have liveValues enabled");
      }
      if (multiValued) {
        f.fail("liveValues", "liveValues fields must not be multiValued");
      }
      if (!ft.stored()) {
        f.fail("liveValues", "this field is not stored");
      }
      if (!idField.fieldType.stored()) {
        f.fail("liveValues", "id field \"" + liveValuesIDField + "\" is not stored");
      }
      if (idField.multiValued) {
        f.fail("liveValues", "id field \"" + liveValuesIDField + "\" must not be multiValued");
      }
      if (!idField.valueType.equals("atom") && !idField.valueType.equals("text")) {
        f.fail("liveValues", "id field \"" + liveValuesIDField + "\" must have type=atom or type=text");
      }
      // TODO: we could relax this, since
      // PostingsHighlighter lets you pull from "private"
      // source:
      if (highlighted) {
        f.fail("liveValues", "cannot highlight live fields");
      }
    } else {
      liveValuesIDField = null;
    }

    // nocommit allow changing indexFieldName for
    // SSDVFacets, too

    String facet = f.getEnum("facet");
    if (facet.equals("hierarchy") && type.equals("atom") && (ft.indexed() || ft.stored())) {
      f.fail("facet", "facet=hierarchy fields cannot have type atom if it's indexed or stored");
    }
    if (facet.equals("numericRange")) {
      if (!type.equals("long") && !type.equals("int") && !type.equals("float") && !type.equals("double")) {
        f.fail("facet", "numericRange facets only applies to numeric types");
      }
      if (!ft.indexed()) {
        f.fail("index", "facet=numericRange fields must have index=true");
      }
      // We index the field as NumericField, for drill-down, and store doc values, for dynamic facet counting
      ft.setDocValueType(DocValuesType.NUMERIC);
    }

    ft.freeze();

    if (facet.equals("no") == false && facet.equals("numericRange") == false) {
      if (facet.equals("hierarchy")) {
        state.facetsConfig.setHierarchical(name, true);
      }
      if (multiValued) {
        state.facetsConfig.setMultiValued(name, true);
      }
    }

    // nocommit facetsConfig.setIndexFieldName
    // nocommit facetsConfig.setRequireDimCount

    return new FieldDef(name, ft, type, facet, pf, dvf, multiValued, sim, indexAnalyzer, searchAnalyzer, highlighted, liveValuesIDField, null, 0.0f, 0l, null);
  }

  /** Messy: we need this for indexed-but-not-tokenized
   *  fields, solely for .getOffsetGap I think. */
  public final static Analyzer dummyAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        throw new UnsupportedOperationException();
      }
    };

  private static List<String> toStringList(List<Object> l) {
    List<String> words = new ArrayList<String>();
    for(Object o : l) {
      words.add((String) o);
    }
    return words;
  }

  final static Pattern COMMENTS_PATTERN = Pattern.compile("#.*$", Pattern.MULTILINE);

  static TokenStreamComponents buildCustomAnalysisChain(Version matchVersion, Request chain, Reader reader) {

    Request t = chain.getStruct("tokenizer");

    Request.PolyResult pr = t.getPoly("class");

    // nocommit charFilters

    Tokenizer tokenizer;
    // nocommit use java7 string switch:
    if (pr.name.equals("StandardTokenizer")) {
      tokenizer = new StandardTokenizer(matchVersion, reader);
      ((StandardTokenizer) tokenizer).setMaxTokenLength(pr.r.getInt("maxTokenLength"));
    } else if (pr.name.equals("WhitespaceTokenizer")) {
      tokenizer = new WhitespaceTokenizer(matchVersion, reader);
    } else if (pr.name.equals("PatternTokenizer")) {
      Pattern p;
      try {
        p = Pattern.compile(pr.r.getString("pattern"));
      } catch (PatternSyntaxException pse) {
        pr.r.fail("pattern", "failed to compile Pattern", pse);
        // Dead code but compiler disagrees:
        p = null;
      }
      tokenizer = new PatternTokenizer(reader, p, pr.r.getInt("group"));
    } else if (pr.name.equals("ICUTokenizer")) {
      final BreakIterator breakers[];
      if (pr.r.hasParam("rules")) {
        breakers = new BreakIterator[UScript.CODE_LIMIT];
        for(Object o : pr.r.getList("rules")) {
          Request r2 = (Request) o;
          String script = r2.getString("script");
          String rules = r2.getString("rules");
          rules = COMMENTS_PATTERN.matcher(rules).replaceAll("");
          int code;
          try {
            code = UCharacter.getPropertyValueEnum(UProperty.SCRIPT, script);
          } catch (IllegalArgumentException iae) {
            r2.fail("script", "failed to parse as script code: " + iae.getMessage());
            // Dead code but compiler disagrees:
            code = -1;
          }
          try {
            breakers[code] = new RuleBasedBreakIterator(rules);
          } catch (IllegalArgumentException iae) {
            r2.fail("rules", "failed to parse rules: " + iae.getMessage());
          }
        }
      } else {
        breakers = null;
      }

      // nocommit: what is this doing, just passing true...
      ICUTokenizerConfig config = new DefaultICUTokenizerConfig(true) {
        
        @Override
        public BreakIterator getBreakIterator(int script) {
          if (breakers[script] != null) {
            return (BreakIterator) breakers[script].clone();
          } else {
            return super.getBreakIterator(script);
          }
        }

        // TODO: we could also allow codes->types mapping
      };

      tokenizer = new ICUTokenizer(reader, config);

    } else {
      // BUG
      tokenizer = null;
      assert false;
    }

    TokenStream last = tokenizer;
    if (chain.hasParam("tokenFilters")) {
      for(Object o : chain.getList("tokenFilters")) {
        Request sub = (Request) o;
        pr = sub.getPoly("class");
        // nocommit use java7 string switch:
        if (pr.name.equals("StandardFilter")) {
          last = new StandardFilter(matchVersion, last);
        } else if (pr.name.equals("EnglishPossessiveFilter")) {
          last = new EnglishPossessiveFilter(matchVersion, last);
        } else if (pr.name.equals("PorterStemFilter")) {
          last = new PorterStemFilter(last);
        } else if (pr.name.equals("ArabicStemFilter")) {
          last = new ArabicStemFilter(last);
        } else if (pr.name.equals("EnglishMinimalStemFilter")) {
          last = new EnglishMinimalStemFilter(last);
        } else if (pr.name.equals("LowerCaseFilter")) {
          last = new LowerCaseFilter(matchVersion, last);
        } else if (pr.name.equals("SetKeywordMarkerFilter")) {
          CharArraySet set = new CharArraySet(matchVersion, toStringList(pr.r.getList("keyWords")), false);
          last = new SetKeywordMarkerFilter(last, set);
        } else if (pr.name.equals("StopFilter")) {
          CharArraySet set = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          last = new StopFilter(matchVersion, last, set);
        } else if (pr.name.equals("SuggestStopFilter")) {
          CharArraySet set = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          last = new SuggestStopFilter(last, set);
        } else if (pr.name.equals("SynonymFilter")) {
          Analyzer a = getAnalyzer(matchVersion, pr.r, "analyzer");
          if (a == null) {
            pr.r.fail("analyzer", "analyzer is required");
          }

          try {
            SynonymMap.Parser parser = new SynonymMap.Parser(true, a) {
                @Override
                public void parse(Reader in) throws IOException {
                  // nocommit move parsing in here!
                };
              };

            CharsRef scratch = new CharsRef();
            CharsRef scratchOutput = new CharsRef();
            for(Object o2 : pr.r.getList("synonyms")) {
              Request syn = (Request) o2;
              boolean replace = syn.getBoolean("replace");
              CharsRef output = new CharsRef(syn.getString("output"));
              if (!syn.isString("input")) {
                for(Object o3 : syn.getList("input")) {
                  parser.add(parser.analyze((String) o3, scratch),
                             output,
                             !replace);
                }
              } else {
                parser.add(parser.analyze(syn.getString("input"), scratch),
                           output,
                           !replace);
              }
            }
            last = new SynonymFilter(last, parser.build(), pr.r.getBoolean("ignoreCase"));
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        } else {
          assert false: "unrecognized: " + pr.name;
        }
      }
    }

    return new TokenStreamComponents(tokenizer, last);
  }

  private static class CustomAnalyzer extends Analyzer {
    private final String json;
    private final Version matchVersion;
    private int positionIncrementGap;
    private int offsetGap;

    public CustomAnalyzer(Version matchVersion, String json) {
      this.matchVersion = matchVersion;
      this.json = json;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      JSONObject o;
      try {
        o = (JSONObject) JSONValue.parseStrict(json);
      } catch (net.minidev.json.parser.ParseException pe) {
        // BUG
        throw new RuntimeException(pe);
      }
      Request r = new Request(null, fieldName, o, (StructType) ANALYZER_TYPE);
      positionIncrementGap = r.getInt("positionIncrementGap");
      offsetGap = r.getInt("offsetGap");
      return buildCustomAnalysisChain(matchVersion,
                                      r,
                                      reader);
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
      return positionIncrementGap;
    }

    @Override
    public int getOffsetGap(String fieldName) {
      return offsetGap;
    }
  }

  static Analyzer getAnalyzer(Version matchVersionGlobal, Request f, String name) {
    Analyzer analyzer;
    if (f.hasParam(name)) {
      Request a = f.getStruct(name);
      String jsonOrig = a.toString();

      Version matchVersion;
      if (a.hasParam("matchVersion")) {
        matchVersion = getVersion(a.getEnum("matchVersion"));
      } else {
        matchVersion = matchVersionGlobal;
      }

      if (a.hasParam("class")) {
        // Predefined analyzer class:
        Request.PolyResult pr = a.getPoly("class");
        // TODO: try to "share" a single instance of
        // each?  Analyzer can be costly!
        if (pr.name.equals("StandardAnalyzer")) {
          CharArraySet set = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          StandardAnalyzer sa = new StandardAnalyzer(matchVersion, set);
          analyzer = sa;
          sa.setMaxTokenLength(pr.r.getInt("maxTokenLength"));
        } else if (pr.name.equals("EnglishAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new EnglishAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new EnglishAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("GermanAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new GermanAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new GermanAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("ArabicAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new ArabicAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new ArabicAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("ArmenianAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new ArmenianAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new ArmenianAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("BasqueAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new BasqueAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new BasqueAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("BrazilianAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new BrazilianAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new BrazilianAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("BulgarianAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new BulgarianAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new BulgarianAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("CatalanAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          if (pr.r.hasParam("stemExclusionSet")) {
            CharArraySet stemExclusions = new CharArraySet(matchVersion, toStringList(pr.r.getList("stemExclusionSet")), false);
            analyzer = new CatalanAnalyzer(matchVersion, stopWords, stemExclusions);
          } else {
            analyzer = new CatalanAnalyzer(matchVersion, stopWords);
          }
        } else if (pr.name.equals("CJKAnalyzer")) {
          CharArraySet stopWords = new CharArraySet(matchVersion, toStringList(pr.r.getList("stopWords")), false);
          analyzer = new CJKAnalyzer(matchVersion, stopWords);
        } else if (pr.name.equals("CollationKeyAnalyzer")) {
          Locale locale = SearchHandler.getLocale(pr.r.getStruct("locale"));
          analyzer = new CollationKeyAnalyzer(matchVersion, Collator.getInstance(locale));
        } else if (pr.name.equals("WhitespaceAnalyzer")) {
          analyzer = new WhitespaceAnalyzer(matchVersion);
        } else {
          f.fail("class", "unrecognized analyzer class \"" + pr.name + "\"");
          // Dead code but compiler disagrees:
          analyzer = null;
        }
      } else if (a.hasParam("tokenizer")) {
        a.getInt("positionIncrementGap");
        a.getInt("offsetGap");
        // Ensures the args are all correct:
        buildCustomAnalysisChain(matchVersion, a, new StringReader(""));
        analyzer = new CustomAnalyzer(matchVersion, jsonOrig);
      } else {
        f.fail(name, "either class or tokenizer/tokenFilters are required");
        analyzer = null;
      }
    } else {
      analyzer = null;
    }

    return analyzer;
  }

  /** Parses a Lucene version constant. */
  @SuppressWarnings("deprecation")
  public static Version getVersion(String v) {
    if (v.equals("LUCENE_40")) {
      return Version.LUCENE_40;
    } else if (v.equals("LUCENE_41")) {
      return Version.LUCENE_41;
    } else if (v.equals("LUCENE_42")) {
      return Version.LUCENE_42;
    } else if (v.equals("LUCENE_43")) {
      return Version.LUCENE_43;
    } else if (v.equals("LUCENE_44")) {
      return Version.LUCENE_44;
    } else if (v.equals("LUCENE_45")) {
      return Version.LUCENE_45;
    } else if (v.equals("LUCENE_46")) {
      return Version.LUCENE_46;
    } else {
      throw new IllegalArgumentException("unhandled version " + v);
    }
  }

  @Override
  public FinishRequest handle(final IndexState state, Request r, Map<String,List<String>> params) throws Exception {

    assert state != null;

    final Map<String,FieldDef> pendingFieldDefs = new HashMap<String,FieldDef>();
    final Map<String,String> saveStates = new HashMap<String,String>();

    if (r.hasParam("fields")) {
      r = r.getStruct("fields");

      Set<String> seen = new HashSet<String>();

      // We make two passes.  In the first pass, we do the
      // "real" fields, and second pass does the virtual
      // fields, so that any fields the virtual field
      // references are guaranteed to exist, in a single
      // request (or, from the saved json):
      for(int pass=0;pass<2;pass++) {
        Iterator<Map.Entry<String,Object>> it = r.getParams();
        while(it.hasNext()) {
          Map.Entry<String,Object> ent = it.next();
          String fieldName = ent.getKey();

          if (pass == 1 && seen.contains(fieldName)) {
            continue;
          }

          if (!(ent.getValue() instanceof JSONObject)) {
            r.fail("field \"" + fieldName + "\": expected object containing the field type but got: " + ent.getValue());
          }

          if (pass == 0 && "virtual".equals(((JSONObject) ent.getValue()).get("type"))) {
            // Do this on 2nd pass so the field it refers to will be registered even if it's a single request
            continue;
          }

          if (!IndexState.isSimpleName(fieldName)) {
            r.fail("invalid field name \"" + fieldName + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
          }

          if (fieldName.endsWith("_boost")) {
            r.fail("invalid field name \"" + fieldName + "\": field names cannot end with _boost");
          }

          if (seen.contains(fieldName)) {
            throw new IllegalArgumentException("field \"" + fieldName + "\" appears at least twice in this request");
          }
      
          seen.add(fieldName);

          JSONObject fd = (JSONObject) ent.getValue();

          saveStates.put(fieldName, fd.toString());

          pendingFieldDefs.put(fieldName, parseOneFieldType(r, state, pendingFieldDefs, fieldName, fd));
        }
      }
    }

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        for(Map.Entry<String,FieldDef> ent : pendingFieldDefs.entrySet()) {
          // Silly: we need JSONObject.clone...
          JSONObject o;
          try {
            o = (JSONObject) JSONValue.parseStrict(saveStates.get(ent.getKey()));
          } catch (net.minidev.json.parser.ParseException pe) {
            // BUG
            assert false;
            throw new RuntimeException(pe);
          }

          state.addField(ent.getValue(), o);
        }

        return state.getAllFieldsJSON();
      }
    };
  }
}
