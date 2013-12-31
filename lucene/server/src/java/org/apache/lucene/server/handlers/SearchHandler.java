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
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.search.CachedOrdsCountingFacetsAggregator;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.DrillSideways;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinCollector;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.search.postingshighlight.PassageFormatter;
import org.apache.lucene.search.postingshighlight.PassageScorer;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.lucene.search.postingshighlight.WholeBreakIterator;
import org.apache.lucene.server.Constants;
import org.apache.lucene.server.FieldDef;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.MultiFacetsAccumulator;
import org.apache.lucene.server.MyIndexSearcher;
import org.apache.lucene.server.RecencyBlendedFieldComparatorSource;
import org.apache.lucene.server.SVJSONPassageFormatter;
import org.apache.lucene.server.TopFacetsCache;
import org.apache.lucene.server.WholeMVJSONPassageFormatter;
import org.apache.lucene.server.params.*;
import org.apache.lucene.server.params.PolyType.PolyEntry;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

/** Handles {@code search}. */
public class SearchHandler extends Handler {

  /** If a browse-only facet request asks for more than this
   *  count then we don't cache it. */
  private final static int TOP_FACET_CACHE_MAX_FACET_COUNT = 100;

  private final static Type SORT_TYPE = new ListType(
                                            new StructType(new Param("field", "The field to sort on.  Pass <code>docid</code> for index order and <code>score</code> for relevance sort.", new StringType()),
                                                           new Param("reverse", "Sort in reverse of the field's natural order", new BooleanType(), false)));

  private final static Type BOOLEAN_OCCUR_TYPE = new EnumType("must", "Clause is required.",
                                                              "should", "Clause is optional.",
                                                              "must_not", "Clause must not match.");

  private final static WrapType FILTER_TYPE_WRAP = new WrapType();
  private final static WrapType QUERY_TYPE_WRAP = new WrapType();

  private final static StructType FILTER_TYPE = new StructType(
                                                               new Param("class", "Filter class",
                                                                         new PolyType(Filter.class,
                                                                                      new PolyEntry("CachingWrapperFilter", "Wraps any other Filter and caches its bitset",
                                                                                                    new Param("id", "Optional id to record this filter; subsequent requests can just refer to the filter by id", new StringType()),
                                                                                                    new Param("filter", "Wrapped filter", FILTER_TYPE_WRAP)),
                                                                                      new PolyEntry("QueryWrapperFilter", "Wraps a Query and creates a Filter",
                                                                                                    new Param("query", "Wrapped query", QUERY_TYPE_WRAP)),
                                                                                      new PolyEntry("BooleanFilter", "A container filter that composes multiple child filters",
                                                                                                    new Param("subFilters", "Child filters",
                                                                                                        new ListType(
                                                                                                            new StructType(
                                                                                                                           new Param("occur", "Occur.", BOOLEAN_OCCUR_TYPE),
                                                                                                                           new Param("filter", "Filter for this clause", FILTER_TYPE_WRAP))))),
                                                                                      new PolyEntry("BooleanFieldFilter", "Accepts all documents indexed with true value in a boolean field",
                                                                                                    new Param("field", "Name of boolean field", new StringType())))));
  static {
    FILTER_TYPE_WRAP.set(FILTER_TYPE);
  }

  private final static StructType QUERY_TYPE = new StructType(
                                 new Param("boost", "Query boost", new FloatType(), 1.0f),
                                 new Param("field", "Which field to use for this query and any sub-queries ", new StringType()),
                                 new Param("class", "Query class",
                                     new PolyType(Query.class,
                                         new PolyEntry("DisjunctionMaxQuery", "A query that generates the union of documents produced by its subqueries, and that scores each document with the maximum score for that document as produced by any subquery, plus a tie breaking increment for any additional matching subqueries (see @lucene:core:org.apache.lucene.search.DisjunctionMaxQuery)",
                                                       new Param("subQueries", "Queries to OR/max together.", new ListType(QUERY_TYPE_WRAP)),
                                                       new Param("tieBreakMultiplier", "Tie break score.", new FloatType(), 0.0f)),
                                         new PolyEntry("BooleanQuery", "A Query that matches documents matching boolean combinations of other queries, e.g (see @lucene:core:org.apache.lucene.search.BooleanQuery)",
                                                       new Param("subQueries", "Queries to OR/max together.",
                                                           new ListType(
                                                               new StructType(
                                                                   new Param("occur", "Occur.", BOOLEAN_OCCUR_TYPE),
                                                                   new Param("query", "Query for this clause", QUERY_TYPE_WRAP)))),
                                                       new Param("disableCoord", "Disable coord factor.", new BooleanType(), true),
                                                       new Param("minimumNumberShouldMatch", "Minimum number of should clauses for a match.", new IntType(), 0)),
                                         new PolyEntry("CommonTermsQuery", "A query that executes high-frequency terms in a optional sub-query to prevent slow queries due to common terms like stopwords (see @lucene:queries:org.apache.lucene.queries.CommonTermsQuery)",
                                                       new Param("terms", "List of terms", new ListType(new StringType())),
                                                       new Param("lowFreqOccur", "BooleanClause.Occur used for low frequency terms", BOOLEAN_OCCUR_TYPE),
                                                       new Param("highFreqOccur", "BooleanClause.Occur used for high frequency terms", BOOLEAN_OCCUR_TYPE),
                                                       new Param("maxTermFrequency", "a value from [0.0, 1.0) or an absolutely number >= 1 representing the maximum threshold of a terms document frequency to be considered a low frequency term", new FloatType()),
                                                       new Param("disableCoord", "True if coord factor should not apply to the low / high frequency clauses", new BooleanType(), false)),
                                         new PolyEntry("ConstantScoreQuery", "A query that wraps another query or a filter and simply returns a constant score equal to the query boost for every document that matches the filter or query (see @lucene:core:org.apache.lucene.search.ConstantScoreQuery)",
                                                       new Param("query", "Wrapped query", QUERY_TYPE_WRAP),
                                                       new Param("filter", "Wrapped filter", FILTER_TYPE)),
                                         new PolyEntry("FuzzyQuery", "Implements the fuzzy search query (see @lucene:core:org.apache.lucene.search.FuzzyQuery)",
                                                       new Param("term", "Term text", new StringType()),
                                                       new Param("maxEdits", "Maximum number of edits; must be 1 or 2", new IntType(), FuzzyQuery.defaultMaxEdits),
                                                       new Param("prefixLength", "Length of the common (non-fuzzy) prefix", new IntType(), FuzzyQuery.defaultPrefixLength),
                                                       new Param("maxExpansions", "Maximum number of terms to match", new IntType(), FuzzyQuery.defaultMaxExpansions),
                                                       new Param("transpositions", "Whether transposition counts as a single edit", new BooleanType(), FuzzyQuery.defaultTranspositions)),
                                         new PolyEntry("MatchAllDocsQuery", "A query that matches all documents. (see @lucene:core:org.apache.lucene.search.MatchAllDocsQuery)"),
                                         new PolyEntry("MultiPhraseQuery", "MultiPhraseQuery is a generalized version of PhraseQuery, with an added method #add(Term[]) (see @lucene:core:org.apache.lucene.search.MultiPhraseQuery)",
                                                       new Param("terms", "List of terms/positions in the phrase",
                                                                 new ListType(new OrType(new StringType(),
                                                                                         new StructType(
                                                                                                        new Param("term", "Term(s) text", new OrType(new ListType(new StringType()),
                                                                                                                                                  new StringType())),
                                                                                                        new Param("position", "Which position this term should appear at", new IntType())),
                                                                                         new ListType(new StringType())))),
                                                       new Param("slop", "The number of other words permitted between words in the phrase.  If this is 0 (the default) then the phrase must be an exact match.", new IntType(), 0)),
                                         new PolyEntry("NumericRangeQuery", "",
                                                       new Param("min", "Minimum value", new AnyType()),
                                                       new Param("minInclusive", "True if minimum value is included", new BooleanType()),
                                                       new Param("max", "Maximum value", new AnyType()),
                                                       new Param("maxInclusive", "True if maximum value is included", new BooleanType())),
                                         new PolyEntry("PhraseQuery", "A Query that matches documents containing a particular sequence of terms (see @lucene:core:org.apache.lucene.search.PhraseQuery)",
                                                       new Param("terms", "List of terms in the phrase", new ListType(new StringType())),
                                                       new Param("slop", "The number of other words permitted between words in the phrase.  If this is 0 (the default) then the phrase must be an exact match.", new IntType(), 0)),
                                         new PolyEntry("PrefixQuery", "A Query that matches documents containing terms with a specified prefix (see @lucene:core:org.apache.lucene.search.PrefixQuery)",
                                                       new Param("term", "Prefix text", new StringType())),
                                         new PolyEntry("RegexpQuery", "A fast regular expression query based on the org.apache.lucene.util.automaton package (see @lucene:core:org.apache.lucene.search.RegexpQuery)",
                                                       new Param("regexp", "Regular expression text", new StringType())),
                                         new PolyEntry("TermRangeQuery", "A Query that matches documents within an range of terms (see @lucene:core:org.apache.lucene.search.TermRangeQuery)",
                                                       new Param("lowerTerm", "Lower term", new StringType()),
                                                       new Param("includeLower", "True if lower term is included", new BooleanType()),
                                                       new Param("upperTerm", "Upper term", new StringType()),
                                                       new Param("includeUpper", "True if upper term is included", new BooleanType())),
                                         new PolyEntry("TermQuery", "A Query that matches documents containing a term (see @lucene:core:org.apache.lucene.search.TermQuery)",
                                                       new Param("term", "Term text", new StringType())),
                                         new PolyEntry("ToParentBlockJoinQuery", "A parent query that wraps another (child) query and joins matches from the child to the parent documents (see @lucene:join:org.apache.lucene.search.ToParentBlockJoinQuery)",
                                                       new Param("childQuery", "Child query", QUERY_TYPE_WRAP),
                                                       new Param("parentsFilter", "Filter identifying parent documents", FILTER_TYPE),
                                                       new Param("scoreMode", "How scores are propogated from children to parent hit",
                                                                 new EnumType(
                                                                              "Avg", "Average all child scores",
                                                                              "Max", "Max of all child scores",
                                                                              "Total", "Sum of all child scores",
                                                                              "None", "Child scores are ignored"), "Max"),
                                                       new Param("childHits", "Whether and how to return child hits for each parent hit",
                                                                 new StructType(
                                                                     new Param("sort", "How to sort child hits", SORT_TYPE),
                                                                     new Param("maxChildren", "Maximum number of children to retrieve per parent", new IntType(), Integer.MAX_VALUE),
                                                                     new Param("trackScores", "Whether to compute scores", new BooleanType(), true),
                                                                     new Param("trackMaxScore", "Whether to compute max score", new BooleanType(), true)))),
                                         new PolyEntry("text", "Parse text into query using default QueryParser.",
                                                       new Param("text", "Query text to parse", new StringType()),
                                                       new Param("defaultField", "Default field for QueryParser", new StringType())),
                                         new PolyEntry("WildcardQuery", "Implements the wildcard search query (see @lucene:core:org.apache.lucene.search.WildcardQuery)",
                                                       new Param("term", "Wildcard text", new StringType())))));


  
  static {
    QUERY_TYPE_WRAP.set(QUERY_TYPE);
  }

  final static Type LOCALE_TYPE = new StructType(new Param("language", "Locale language", new StringType()),
                                                 new Param("country", "Locale country", new StringType()),
                                                 new Param("variant", "Locale variant", new StringType()));

  private final static Type QUERY_PARSER_TYPE = new StructType(
                      new Param("defaultOperator", "Whether terms are OR'd or AND'd by default.",
                                new EnumType("or", "or", "and", "and"), "or"),  
                      new Param("fuzzyMinSim", "Minimum similarity for fuzzy queries", new IntType(), LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE),
                      new Param("fuzzyPrefixLength", "Prefix length for fuzzy queries", new IntType(), FuzzyQuery.defaultPrefixLength),
                      new Param("phraseSlop", "Default slop for phrase queries", new IntType(), 0),
                      new Param("enablePositionIncrements", "When set, phrase and multi-phrase queries will be aware of position increments", new BooleanType(), true),
                      new Param("lowercaseExpandedTerms", "Whether terms of wildcard, prefix, fuzzy and range queries should be automatically lowercased", new BooleanType(), true),
                      new Param("locale", "Locale to be used by date range parsing, lowercasing and other locale-sensitive operations", LOCALE_TYPE),
                      new Param("class", "Which QueryParser to use.",
                                new PolyType(QueryParser.class,
                                             new PolyEntry("MultiFieldQueryParser", "",
                                                           new Param("fields", "Which fields/boosts to query against",
                                                                     new ListType(new OrType(new StringType(),
                                                                                             new StructType(
                                                                                                            new Param("field", "Field name", new StringType()),
                                                                                                            new Param("boost", "Field boost", new FloatType())))))),
                                             new PolyEntry("classic", "Classic QueryParser.",
                                                           new Param("defaultField", "Default field to query against.", new StringType()))),
                                "classic")
                      );

  private final static StructType TYPE =
    new StructType(
        new Param("indexName", "Which index to search", new StringType()),
        new Param("timeStamp", "The 'current' timestamp to use, e.g. for blended sorting; typically this should be fixed and reused from the returned value from the first query in a session", new LongType()),
        new Param("queryText", "Query text to parse using the specified QueryParser.", new StringType()),
        new Param("filter", "Filter to apply to search", FILTER_TYPE),
        new Param("queryParser", "Which QueryParser to use; by default MultiFieldQUeryParser searching all indexed fields will be used", QUERY_PARSER_TYPE),
        new Param("highlighter", "Highlighter configuration to use when highlighting matches.",
                  new StructType(
                      new Param("maxPassages", "Maximum number of passages to extract.", new IntType(), 2),
                      new Param("class", "Which implementation to use",
                                new PolyType(Object.class,
                                             new PolyEntry("PostingsHighlighter", "PostingsHighlighter",
                                                           new Param("maxLength", "Only highlight the first N bytes of each item.", new IntType(), PostingsHighlighter.DEFAULT_MAX_LENGTH),
                                                           new Param("maxSnippetLength", "Maximum length (in chars) for each text snippet chunk.", new IntType(), 100),
                                                           new Param("passageScorer.b", "Scoring factor b in PassageScorer", new FloatType(), 0.75f),
                                                           new Param("passageScorer.k1", "Scoring factor k1 in PassageScorer", new FloatType(), 0.8f),
                                                           new Param("passageScorer.pivot", "Scoring factor pivot in PassageScorer", new FloatType(), 87f),
                                                           //new Param("passageScorer.proxScoring", "True if proximity passage scoring should be used", new BooleanType(), true),
                                                           new Param("breakIterator", "Which BreakIterator to use to create fragments",
                                                               new StructType(
                                                                   new Param("locale", "Locale", SearchHandler.LOCALE_TYPE),
                                                                   new Param("mode", "Which BreakIterator to create",
                                                                             new EnumType("character", "Character instance",
                                                                                          "word", "Word instance",
                                                                                          "line", "Line instance",
                                                                                          "sentence", "Sentence instance"),
                                                                             "sentence")))
                                                           )),
                                "PostingsHighlighter"))),
        new Param("query", "Full query to execute (not using QueryParser).", QUERY_TYPE),
        new Param("grouping", "Whether/how to group search results.",
                  new StructType(
                                 new Param("field", "Field to group by.", new StringType()),
                                 new Param("groupStart", "Which group to start from (for pagination).", new IntType(), 0),
                                 new Param("groupsPerPage", "How many groups to include on each page.", new IntType(), 3),
                                 new Param("hitsPerGroup", "How many top hits to include in each group.", new IntType(), 4),
                                 new Param("doMaxScore", "Whether to compute maxScore for each group.", new BooleanType(), false),
                                 new Param("doDocScores", "Whether to compute scores for each hit in each group.", new BooleanType(), true),
                                 new Param("sort", "How to sort groups (default is by relevance).", SORT_TYPE),
                                 new Param("doTotalGroupCount", "If true, return the total number of groups (at possibly highish added CPU cost)",
                                           new BooleanType(), false))),
        new Param("searcher", "Specific searcher version to use for searching.  There are three different ways to specify a searcher version.",
            new StructType(
                new Param("indexGen", "Search a generation previously returned by an indexing operation such as #addDocument.  Use this to search a non-committed (near-real-time) view of the index.", new LongType()),
                new Param("snapshot", "Search a snapshot previously created with #createSnapshot", new StringType()),
                new Param("version", "Search a specific searcher version.  This is typically used by follow-on searches (e.g., user clicks next page, drills down, or changes sort, etc.) to get the same searcher used by the original search.", new LongType()))),
        new Param("startHit", "Which hit to start from (for pagination).", new IntType(), 0),
        new Param("topHits", "How many top hits to retrieve.", new IntType(), 10),
        new Param("searchAfter", "Only return hits after the specified hit; this is useful for deep paging",
                  new StructType(
                                 new Param("lastDoc", "Last docID of the previous page.", new IntType()),
                                 new Param("lastFieldValues", "Last sort field values of the previous page.", new ListType(new AnyType())),
                                 new Param("lastScore", "Last score of the previous page.", new FloatType()))),
        new Param("retrieveFields", "Which fields to highlight or retrieve.",
                  new ListType(
                      new OrType(
                             new StringType(),
                             new StructType(
                                   new Param("field", "Name of the field in the index.", new StringType()),
                                   //new Param("label", "Label to be used for this value in the returned result, in case you want to retrieve the same field more than once, e.g. one time with highlighting and another time without, or with different highlighting params.", new StringType()),
                                   new Param("highlight", "Whether and how to highlight this field",
                                             new EnumType("no", "No highlighting",
                                                          "whole", "The entire field value(s) is/are highlighted and returned",
                                                          "snippets", "Snippets are extracted from the field value(s); multi-valued fields are concatenated and treated as a single text"),
                                             "no"),
                                   // TODO: make other highlighter config per-field too (maxLength, scoring, proxScoring)
                                   // TODO: factor out & share w/ top-level type
                                   new Param("maxPassages", "Maximum number of passages to extract for this field", new IntType(), 2),
                                   new Param("breakIterator", "Which BreakIterator to use to create fragments",
                                             new StructType(
                                                            new Param("locale", "Locale", SearchHandler.LOCALE_TYPE),
                                                            new Param("mode", "Which BreakIterator to create",
                                                                      new EnumType("character", "Character instance",
                                                                                   "word", "Word instance",
                                                                                   "line", "Line instance",
                                                                                   "sentence", "Sentence instance"),
                                                                      "sentence")))

                                            )))),
        new Param("facets", "Which facets to retrieve.",
                  new ListType(
                               new StructType(
                                   new Param("path", "Prefix path to facet 'under'.",
                                             new OrType(new StringType(), new ListType(new StringType()))),
                                   new Param("numericRanges", "Custom numeric ranges.  Field must be indexed with facet=numericRange.",
                                       new ListType(
                                           new StructType(new Param("label", "Label for this range", new StringType()),
                                                          new Param("min", "Min value for the range", new LongType()),
                                                          new Param("minInclusive", "True if the min value is inclusive", new BooleanType()),
                                                          new Param("max", "Max value for the range", new LongType()),
                                                          new Param("maxInclusive", "True if the max value is inclusive", new BooleanType())))),
                                   new Param("autoDrillDown", "True if single-child facet should be auto-expanded (not yet implemented!).", new BooleanType()),
                                   new Param("useOrdsCache", "True if the ordinals cache should be used.", new BooleanType(), false),
                                   new Param("topN", "How many top facets to keep.", new IntType(), 10)))),
        new Param("drillDowns", "Facet drill down filters to apply.",
                  new ListType(new StructType(
                                   new Param("field", "Field name to drill down on.", new StringType()),
                                   new Param("values", "Which (OR'd) values to allow.  If an element a String, it's a flat facet value; if it's a list of String then it's a hierarchy path.",
                                             new ListType(new OrType(new StringType(),
                                                                     new ListType(new StringType()))))))),
        new Param("sort", "Sort hits by field (default is by relevance).",
                  new StructType(
                      new Param("doMaxScore", "Compute the max score across all hits (costs added CPU).", new BooleanType(), false),
                      new Param("doDocScores", "Compute the doc score for each collected (costs added CPU).", new BooleanType(), false),
                      new Param("fields", "Fields to sort on.", SORT_TYPE)))
                   );
  @Override
  public String getTopDoc() {
    return "Execute a search.";
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  /** Sole constructor. */
  public SearchHandler(GlobalState state) {
    super(state);
  }

  /** Holds the state for a single {@link
   *  PostingsHighlighter} instance. */
  public final class HighlighterConfig {
    PostingsHighlighter highlighter;
    int maxPassages;
    String config;

    /** Sole constructor. */
    public HighlighterConfig() {
    }

    /** Holds field values for all documents we loaded for
     * this request. */
    Map<Integer,Map<String,Object>> docsCache = new HashMap<Integer,Map<String,Object>>();

    /** Load the fiels for a single document, using the
     *  cache so we only ever load a document once from
     *  Lucene. */
    @SuppressWarnings("unchecked")
    public Map<String,Object> getDocument(IndexState state, IndexSearcher searcher, int docID) throws IOException {
      Map<String,Object> doc = docsCache.get(docID);
      if (doc == null) {
        // Load & cache all stored fields:
        doc = new HashMap<String,Object>();
        docsCache.put(docID, doc);
        for(StorableField field : searcher.doc(docID)) {
          String name = field.name();
          FieldDef fd = state.getField(name);
          Object value = field.numericValue();
          if (value == null) {
            value = field.stringValue();
          }
          Object o = doc.get(name);
          if (o == null) {
            if (fd.multiValued) {
              List<Object> l = new ArrayList<Object>();
              l.add(value);
              value = l;
            }
            doc.put(name, value);
          } else {
            assert o instanceof List;
            ((List<Object>) o).add(value);
          }
        }
      }
      return doc;
    }
  }

  /** Loads docs/fields from a shared cache so we only call
   *  IndexReader.document once per hit */
  private static class CachedDocsJSONPostingsHighlighter extends PostingsHighlighter {
    private final HighlighterConfig config;
    private final IndexState state;
    private final Map<String,FieldHighlightConfig> perFieldConfig;
    private final PassageScorer scorer;
    private final BreakIterator defaultBI;
    private final int maxSnippetLength;

    public CachedDocsJSONPostingsHighlighter(IndexState state, HighlighterConfig config,
                                             Map<String,FieldHighlightConfig> perFieldConfig,
                                             int maxLength, int maxSnippetLength, PassageScorer scorer, BreakIterator defaultBI) {
      super(maxLength);
      this.maxSnippetLength = maxSnippetLength;
      this.perFieldConfig = perFieldConfig;
      this.config = config;
      this.scorer = scorer;
      this.state = state;
      this.defaultBI = defaultBI;
    }

    @Override
    protected PassageScorer getScorer(String fieldName) {
      return scorer;
    }

    @Override
    protected BreakIterator getBreakIterator(String fieldName) {
      FieldHighlightConfig perField = perFieldConfig.get(fieldName);
      if (perField.mode.equals("whole")) {
        return new WholeBreakIterator();
      } else if (perField.breakIterator != null) {
        return perField.breakIterator;
      } else {
        return defaultBI;
      }
    }

    @Override
    protected PassageFormatter getFormatter(String fieldName) {
      FieldHighlightConfig perField = perFieldConfig.get(fieldName);
      if (perField.multiValued && perField.mode.equals("whole")) {
        FieldDef fd = state.getField(fieldName);
        assert fd.indexAnalyzer != null;
        return new WholeMVJSONPassageFormatter(fd.indexAnalyzer.getOffsetGap(fieldName));
      } else {
        return new SVJSONPassageFormatter(maxSnippetLength);
      }
    }

    // TODO: allow pulling from DV too:

    @SuppressWarnings("unchecked")
    @Override
    protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docIDs, int maxLength) throws IOException {
      String[][] contents = new String[fields.length][docIDs.length];
      for (int i = 0; i < docIDs.length; i++) {
        Map<String,Object> doc = config.getDocument(state, searcher, docIDs[i]);

        for (int j = 0; j < fields.length; j++) {
          Object o = doc.get(fields[j]);
          FieldHighlightConfig perField = perFieldConfig.get(fields[j]);
          boolean isWhole = perField.mode.equals("whole");
          if (o != null) {
            String value;
            if (o instanceof List) {
              StringBuilder sb = new StringBuilder();
              for(Object _s : (List<Object>) o) {
                // Schema enforces only text/atom fields can
                // be highlighted:
                assert _s instanceof String;
                String s = (String) _s;

                if (sb.length() > 0) {
                  if (isWhole) {
                    // NOTE: we ensured this character does
                    // not occur in the content, at indexing
                    // time.  We could remove this restriction
                    // by looking at the actual String[]
                    // values for the field while formatting
                    // the passages:
                    sb.append(Constants.INFORMATION_SEP);
                  } else {
                    sb.append(' ');
                  }
                }
                
                if (sb.length() + s.length() > maxLength) {
                  sb.append(s, 0, maxLength - sb.length());
                  break;
                } else {
                  sb.append(s);
                }
              }
              value = sb.toString();
            } else {
              String s = (String) o;
              if (s.length() > maxLength) {
                value = s.substring(0, maxLength);
              } else {
                value = s;
              }
            }
            contents[j][i] = value;
          } else {
            contents[j][i] = "";
          }
        }
      }

      return contents;
    }
  }

  private static BreakIterator parseBreakIterator(Request r) {
    BreakIterator bi;
    if (r.hasParam("breakIterator")) {
      Request r2 = r.getStruct("breakIterator");
      Locale locale = getLocale(r2.getStruct("locale"));
      String mode = r.getString("mode");
      if (mode.equals("character")) {
        bi = BreakIterator.getCharacterInstance(locale);
      } else if (mode.equals("word")) {
        bi = BreakIterator.getWordInstance(locale);
      } else if (mode.equals("line")) {
        bi = BreakIterator.getLineInstance(locale);
      } else if (mode.equals("sentence")) {
        bi = BreakIterator.getSentenceInstance(locale);
      } else {
        assert false;
        bi = null;
      }
    } else {
      bi = BreakIterator.getSentenceInstance(Locale.ROOT);
    }
    return bi;
  }

  private HighlighterConfig getHighlighter(IndexState state, Request r, Map<String,FieldHighlightConfig> highlightFields) {
    HighlighterConfig config = new HighlighterConfig();

    if (r.hasParam("highlighter")) {
      r = r.getStruct("highlighter");
      config.maxPassages = r.getInt("maxPassages");

      final Request.PolyResult pr = r.getPoly("class");
      if (!pr.name.equals("PostingsHighlighter")) {
        r.fail("class", "Only PostingsHighlighter is currently supported.");
      } 

      r = pr.r;

      //final boolean doProx = r.getBoolean("passageScorer.proxScoring");

      // TODO: BreakIterator Locale?
      PassageScorer scorer = new PassageScorer(r.getFloat("passageScorer.k1"), r.getFloat("passageScorer.b"), r.getFloat("passageScorer.pivot"));

      final BreakIterator bi = parseBreakIterator(r);
      config.highlighter = new CachedDocsJSONPostingsHighlighter(state,
                                                                 config,
                                                                 highlightFields,
                                                                 r.getInt("maxLength"),
                                                                 r.getInt("maxSnippetLength"),
                                                                 scorer,
                                                                 bi);
    } else {
      // Default:
      config.maxPassages = 2;
      config.highlighter = new CachedDocsJSONPostingsHighlighter(state,
                                                                 config,
                                                                 highlightFields,
                                                                 PostingsHighlighter.DEFAULT_MAX_LENGTH,
                                                                 100,
                                                                 new PassageScorer(),
                                                                 BreakIterator.getSentenceInstance(Locale.ROOT));
    }

    return config;
  }

  private Sort parseSort(long timeStamp, IndexState state, List<Object> fields) {
    List<SortField> sortFields = new ArrayList<SortField>();
    for(Object _sub : fields) {
      Request sub = (Request) _sub;

      String fieldName = sub.getString("field");
      SortField sf;
      if (fieldName.equals("docid")) {
        sf = SortField.FIELD_DOC;
      } else if (fieldName.equals("score")) {
        sf = SortField.FIELD_SCORE;
      } else {
        FieldDef fd;
        try {
          fd = state.getField(fieldName);
        } catch (IllegalArgumentException iae) {
          sub.fail("field", iae.toString());
          // Dead code but compiler disagrees:
          fd = null;
        }

        if (fd.blendFieldName != null) {
          sf = new SortField(fd.name, new RecencyBlendedFieldComparatorSource(fd.blendFieldName, fd.blendMaxBoost, timeStamp, fd.blendRange), sub.getBoolean("reverse"));
        } else {

          if (fd.fieldType.docValueType() == null) {
            sub.fail("field", "field \"" + fieldName + "\" was not registered with sort=true");
          }

          SortField.Type sortType;
          if (fd.valueType.equals("atom")) {
            sortType = SortField.Type.STRING;
          } else if (fd.valueType.equals("long")) {
            sortType = SortField.Type.LONG;
          } else if (fd.valueType.equals("int")) {
            sortType = SortField.Type.INT;
          } else if (fd.valueType.equals("double")) {
            sortType = SortField.Type.DOUBLE;
          } else if (fd.valueType.equals("float")) {
            sortType = SortField.Type.FLOAT;
          } else {
            sub.fail("field", "cannot sort by field \"" + fieldName + "\": type is " + fd.valueType);
            assert false;
            sortType = null;
          }

          sf = new SortField(fieldName,
                             sortType,
                             sub.getBoolean("reverse"));
        }
      }
      sortFields.add(sf);
    }

    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  private static Object convertType(FieldDef fd, Object o) {
    if (fd.valueType.equals("boolean")) {
      if (((Integer) o).intValue() == 1) {
        return Boolean.TRUE;
      } else {
        assert ((Integer) o).intValue() == 0;
        return Boolean.FALSE;
      }
      //} else if (fd.valueType.equals("float") && fd.fieldType.docValueType() == DocValuesType.NUMERIC) {
      // nocommit not right...
      //return Float.intBitsToFloat(((Number) o).intValue());
    } else {
      return o;
    }
  }

  @SuppressWarnings("unchecked")
  /** Fills in the returned fields (some hilited) for one hit: */
  private void fillFields(IndexState state, HighlighterConfig highlighter, IndexSearcher s,
                          JSONObject result, ScoreDoc hit, Set<String> fields,
                          Map<String,String[]> highlights,
                          int hiliteHitIndex, Sort sort) throws IOException {
    //System.out.println("fillFields fields=" + fields);
    if (fields != null) {

      // Add requested stored fields (no highlighting):

      // TODO: make this also retrieve DV fields
      // even if they were not stored ...
      Map<String,Object> doc = highlighter.getDocument(state, s, hit.doc);

      for (String name : fields) {
        FieldDef fd = state.getField(name);

        // We detect invalid field above:
        assert fd != null;

        Object v = doc.get(name);
        if (v != null) {
          // We caught same field name above:
          assert !result.containsKey(name);

          if (fd.multiValued == false) {
            result.put(name, convertType(fd, v));
          } else {
            JSONArray arr = new JSONArray();
            result.put(name, arr);
            if (!(v instanceof List)) {
              arr.add(convertType(fd, v));
            } else {
              for(Object o : (List<Object>) v) {
                arr.add(convertType(fd, o));
              }
            }
          }
        }
      }
    }

    if (highlights != null) {
      for (Map.Entry<String,String[]> ent : highlights.entrySet()) {
        String v = ent.getValue()[hiliteHitIndex];
        if (v != null) {
          try {
            result.put(ent.getKey(), JSONValue.parseStrict(v));
          } catch (ParseException pe) {
            // BUG
            throw new RuntimeException(pe);
          }
        }
      }
    }

    if (hit instanceof FieldDoc) {
      FieldDoc fd = (FieldDoc) hit;
      if (fd.fields != null) {
        JSONObject o4 = new JSONObject();
        result.put("sortFields", o4);
        SortField[] sortFields = sort.getSort();
        for(int i=0;i<sortFields.length;i++) {
          if (fd.fields[i] instanceof BytesRef) {
            o4.put(sortFields[i].getField(),
                   ((BytesRef) fd.fields[i]).utf8ToString());
          } else {
            String field = sortFields[i].getField();
            if (field == null) {
              if (sortFields[i] == SortField.FIELD_DOC) {
                field = "docid";
              } else if (sortFields[i] == SortField.FIELD_SCORE) {
                field = "score";
              } else {
                throw new AssertionError();
              }
            }
            o4.put(field, fd.fields[i]);
          }
        }
      }
    }
  }

  private static BooleanClause.Occur parseBooleanOccur(String occurString) {
    if (occurString.equals("should")) {
      return BooleanClause.Occur.SHOULD;
    } else if (occurString.equals("must")) {
      return BooleanClause.Occur.MUST;
    } else if (occurString.equals("must_not")) {
      return BooleanClause.Occur.MUST_NOT;
    } else {
      // BUG
      assert false;
      return null;
    }
  }

  private Filter parseFilter(long timeStamp, Request topRequest, IndexState state, Request r) {
    Filter f;

    Request.PolyResult pr = r.getPoly("class");
    if (pr.name.equals("BooleanFieldFilter")) {
      FieldDef fd = state.getField(pr.r, "field");
      if (!fd.valueType.equals("boolean")) {
        pr.r.fail("field", "field \"" + fd.name + "\" is not a boolean field");
      }
      f = new QueryWrapperFilter(new TermQuery(new Term(fd.name, "1")));
    } else if (pr.name.equals("QueryWrapperFilter")) {
      return new QueryWrapperFilter(parseQuery(timeStamp, topRequest, state, pr.r.getStruct("query"), null, null));
    } else if (pr.name.equals("BooleanFilter")) {
      BooleanFilter bf = new BooleanFilter();
      for (Object o : pr.r.getList("subFilters")) {
        Request sub = (Request) o;
        bf.add(parseFilter(timeStamp, topRequest, state, sub.getStruct("filter")),
               parseBooleanOccur(sub.getEnum("occur")));
      }
      f = bf;
    } else if (pr.name.equals("CachingWrapperFilter")) {
      // TODO: maybe a separate method to enroll a cached filter?
      if (pr.r.hasParam("id") && !pr.r.hasParam("filter")) {
        // ID and no filter: lookup previously registered filter:
        String id = pr.r.getString("id");
        f = state.cachedFilters.get(id);
        if (f == null) {
          pr.r.fail("id", "no CachingWrapperFilter previously registered with id=\"" + id + "\"");
        }
      } else {
        // Has filter or doesn't have ID:
        Request fr = pr.r.getStruct("filter");
        String id;
        if (pr.r.hasParam("id")) {
          id = pr.r.getString("id");
        } else {
          id = toJSONKey(fr);
        }
        f = state.cachedFilters.get(id);
        if (f == null) {
          CachingWrapperFilter cwf = new CachingWrapperFilter(parseFilter(timeStamp, topRequest, state, fr)) {
              @Override
              protected DocIdSet cacheImpl(DocIdSetIterator iterator, AtomicReader reader) throws IOException {
                // nocommit let caller control which Bits
                // impl to use:
                FixedBitSet bits = new FixedBitSet(reader.maxDoc());
                bits.or(iterator);
                return bits;
              }
            };
          // TODO: need clearCachedFilter method
          state.cachedFilters.put(id, cwf);
          f = cwf;
        } else {
          // Else Request is angry that we did not recurse
          // into filter and parse all args:
          pr.r.clearParam("filter");
        }
      }
    } else {
      throw new IllegalArgumentException("unhandled filter class " + pr.name);
    }

    return f;
  }

  static String toJSONKey(Request r) {
    return sort(r.getRawParams()).toString();
  }

  static Object sort(JSONObject o) {
    Map<String,Object> sorted = new TreeMap<String,Object>();
    sorted.putAll(o);
    return sorted;
  }

  /** Records configuration for a block join query. */
  static class BlockJoinQueryChild {
    public Sort sort;
    public int maxChildren;
    public boolean trackScores;
    public boolean trackMaxScore;
  }

  @SuppressWarnings("unchecked")
  private Query parseQuery(long timeStamp, Request topRequest, IndexState state, Request r, String field, Map<ToParentBlockJoinQuery,BlockJoinQueryChild> useBlockJoinCollector) {
    Query q;
    Request.PolyResult pr = r.getPoly("class");
    if (r.hasParam("field")) {
      FieldDef fd = state.getField(r, "field");
      field = fd.name;
      if (!fd.fieldType.indexed()) {
        r.fail("field", "field \"" + field + "\" was not registered with index=true; cannot search");
      }
      if (fd.valueType.equals("boolean")) {
        r.fail("field", "field \"" + field + "\" is boolean: can only create filters with it");
      }
    }

    if (pr.name.equals("BooleanQuery")) {
      Request r2 = pr.r;
      BooleanQuery bq = new BooleanQuery(r2.getBoolean("disableCoord"));
      bq.setMinimumNumberShouldMatch(r2.getInt("minimumNumberShouldMatch"));
      for(Object o : r2.getList("subQueries")) {
        Request r3 = (Request) o;
        BooleanClause.Occur occur = parseBooleanOccur(r3.getEnum("occur"));
        bq.add(parseQuery(timeStamp, topRequest, state, r3.getStruct("query"), field, useBlockJoinCollector), occur);
      }
      q = bq;
    } else if (pr.name.equals("CommonTermsQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      CommonTermsQuery ctq = new CommonTermsQuery(parseBooleanOccur(pr.r.getEnum("highFreqOccur")),
                                                  parseBooleanOccur(pr.r.getEnum("lowFreqOccur")),
                                                  pr.r.getFloat("maxTermFrequency"),
                                                  pr.r.getBoolean("disableCoord"));
      for(Object o : pr.r.getList("terms")) {
        ctq.add(new Term(field, (String) o));
      }
      q = ctq;
    } else if (pr.name.equals("ConstantScoreQuery")) {
      if (pr.r.hasParam("query")) {
        q = new ConstantScoreQuery(parseQuery(timeStamp, topRequest, state, pr.r.getStruct("query"), field, useBlockJoinCollector));
      } else {
        q = new ConstantScoreQuery(parseFilter(timeStamp, topRequest, state, pr.r.getStruct("filter")));
      }
    } else if (pr.name.equals("FuzzyQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      q = new FuzzyQuery(new Term(field, pr.r.getString("term")),
                         pr.r.getInt("maxEdits"),
                         pr.r.getInt("prefixLength"),
                         pr.r.getInt("maxExpansions"),
                         pr.r.getBoolean("transpositions"));
    } else if (pr.name.equals("MatchAllDocsQuery")) {
      q = new MatchAllDocsQuery();
    } else if (pr.name.equals("MultiPhraseQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      MultiPhraseQuery mpq = new MultiPhraseQuery();
      for(Object o : pr.r.getList("terms")) {
        if (o instanceof String) {
          mpq.add(new Term(field, (String) o));
        } else if (o instanceof List) {
          List<Object> terms = (List<Object>) o;
          Term[] termsArray = new Term[terms.size()];
          for(int i=0;i<termsArray.length;i++) {
            termsArray[i] = new Term(field, (String) terms.get(i));
          }
          mpq.add(termsArray);
        } else {
          Request sr = (Request) o;
          int pos = sr.getInt("position");
          if (sr.isString("term")) {
            mpq.add(new Term[] {new Term(field, sr.getString("term"))}, pos);
          } else  {
            List<Object> terms = (List<Object>) o;
            Term[] termsArray = new Term[terms.size()];
            for(int i=0;i<termsArray.length;i++) {
              termsArray[i] = new Term(field, (String) terms.get(i));
            }
            mpq.add(new Term[] {new Term(field, sr.getString("term"))}, pos);
          }
        }
      }
      mpq.setSlop(pr.r.getInt("slop"));
      q = mpq;
    } else if (pr.name.equals("PhraseQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      PhraseQuery pq = new PhraseQuery();
      for(Object o : pr.r.getList("terms")) {
        pq.add(new Term(field, (String) o));
      }
      pq.setSlop(pr.r.getInt("slop"));
      q = pq;
    } else if (pr.name.equals("PrefixQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      // TODO: change rewrite method?
      q = new PrefixQuery(new Term(field, pr.r.getString("term")));
    } else if (pr.name.equals("RegexpQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      // TODO: flags
      q = new RegexpQuery(new Term(field, pr.r.getString("regexp")));
    } else if (pr.name.equals("TermRangeQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      q = new TermRangeQuery(field,
                             new BytesRef(pr.r.getString("lowerTerm")),
                             new BytesRef(pr.r.getString("upperTerm")),
                             pr.r.getBoolean("includeLower"),
                             pr.r.getBoolean("includeUpper"));
    } else if (pr.name.equals("NumericRangeQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      FieldDef fd = state.getField(field);
      NumericType nt = fd.fieldType.numericType();
      if (nt == null) {
        pr.r.fail("field \"" + field + "\" was not registered with numeric type; cannot run NumericRangeQuery");
      }
      Number min;
      if (pr.r.hasParam("min")) {
        Object o = pr.r.getAny("min");
        if (!(o instanceof Number)) {
          pr.r.fail("min", "expected number but got " + o);
        }
        min = (Number) o;
      } else {
        min = null;
      }

      Number max;
      if (pr.r.hasParam("max")) {
        Object o = pr.r.getAny("max");
        if (!(o instanceof Number)) {
          pr.r.fail("max", "expected number but got " + o);
        }
        max = (Number) o;
      } else {
        max = null;
      }

      boolean maxInclusive = true;
      boolean minInclusive = true;
      if (min == null && max == null) {
        pr.r.fail("min", "at least one of min or max is required");
      } else if (min == null) {
        maxInclusive = pr.r.getBoolean("maxInclusive");
      } else if (max == null) {
        minInclusive = pr.r.getBoolean("minInclusive");
      } else {
        maxInclusive = pr.r.getBoolean("maxInclusive");
        minInclusive = pr.r.getBoolean("minInclusive");
      }
      
      if (nt == NumericType.INT) {
        q = NumericRangeQuery.newIntRange(field, toInt(min), toInt(max),
                                          minInclusive, maxInclusive);
      } else if (nt == NumericType.LONG) {
        q = NumericRangeQuery.newLongRange(field, toLong(min), toLong(max),
                                           minInclusive, maxInclusive);
      } else if (nt == NumericType.FLOAT) {
        q = NumericRangeQuery.newFloatRange(field, toFloat(min), toFloat(max),
                                            minInclusive, maxInclusive);
      } else if (nt == NumericType.DOUBLE) {
        q = NumericRangeQuery.newDoubleRange(field, toDouble(min), toDouble(max),
                                             minInclusive, maxInclusive);
      } else {
        // BUG
        assert false;
        q = null;
      }
    } else if (pr.name.equals("TermQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      q = new TermQuery(new Term(field, pr.r.getString("term")));
    } else if (pr.name.equals("ToParentBlockJoinQuery")) {
      Query childQuery = parseQuery(timeStamp, topRequest, state, pr.r.getStruct("childQuery"), field, useBlockJoinCollector);
      Filter parentsFilter = parseFilter(timeStamp, topRequest, state, pr.r.getStruct("parentsFilter"));
      String scoreModeString = pr.r.getEnum("scoreMode");
      ScoreMode scoreMode;
      if (scoreModeString.equals("None")) {
        scoreMode = ScoreMode.None;
      } else if (scoreModeString.equals("Avg")) {
        scoreMode = ScoreMode.Avg;
      } else if (scoreModeString.equals("Max")) {
        scoreMode = ScoreMode.Max;
      } else if (scoreModeString.equals("Total")) {
        scoreMode = ScoreMode.Total;
      } else {
        assert false;
        scoreMode = null;
      }
      q = new ToParentBlockJoinQuery(childQuery, parentsFilter, scoreMode);
      if (pr.r.hasParam("childHits")) {
        if (useBlockJoinCollector == null) {
          pr.r.fail("returnChildHits", "cannot return child hits when inside a filter");
        }
        if (!useBlockJoinCollector.isEmpty()) {
          pr.r.fail("returnChildHits", "can only support a single ToParentBlockJoinQuery for now");
        }
        Request childHits = pr.r.getStruct("childHits");
        BlockJoinQueryChild child = new BlockJoinQueryChild();
        if (childHits.hasParam("sort")) {
          child.sort = parseSort(timeStamp, state, childHits.getList("sort"));
        }
        child.maxChildren = childHits.getInt("maxChildren");
        child.trackScores = childHits.getBoolean("trackScores");
        child.trackMaxScore = childHits.getBoolean("trackMaxScore");
        useBlockJoinCollector.put((ToParentBlockJoinQuery) q, child);
      }
    } else if (pr.name.equals("DisjunctionMaxQuery")) {
      Request r2 = pr.r;
      List<Object> subQueries = r2.getList("subQueries");
      DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(r2.getFloat("tieBreakMultiplier"));
      q = dmq;
      for(Object o : subQueries) {
        dmq.add(parseQuery(timeStamp, topRequest, state, (Request) o, field, useBlockJoinCollector));
      }
    } else if (pr.name.equals("text")) {
      Request r2 = pr.r;
      String queryText = r2.getString("text");
      //System.out.println("parseQuery text=" + queryText + " field=" + field);
      if (field == null) {
        r.fail("no field specified");
      }
      QueryParser queryParser = createQueryParser(state, topRequest, field);
      try {
        q = queryParser.parse(queryText);
      } catch (Exception e) {
        r2.fail("text", "could not parse", e);
        // dead code but compiler disagrees:
        return null;
      }
      //System.out.println("  got: " +q);
    } else if (pr.name.equals("WildcardQuery")) {
      if (field == null) {
        r.fail("no field specified");
      }
      q = new WildcardQuery(new Term(field, pr.r.getString("term")));
    } else {
      q = null;
      assert false;
    }
    q.setBoost(r.getFloat("boost"));

    return q;
  }

  private static Integer toInt(Number x) {
    if (x == null) {
      return null;
    } else {
      return x.intValue();
    }
  }

  private static Long toLong(Number x) {
    if (x == null) {
      return null;
    } else {
      return x.longValue();
    }
  }

  private static Float toFloat(Number x) {
    if (x == null) {
      return null;
    } else {
      return x.floatValue();
    }
  }

  private static Double toDouble(Number x) {
    if (x == null) {
      return null;
    } else {
      return x.doubleValue();
    }
  }

  /** If field is non-null it overrides any specified
   *  defaultField. */
  private QueryParser createQueryParser(IndexState state, Request r, String field) {

    if (r.hasParam("queryParser")) {
      r = r.getStruct("queryParser");
      Request.PolyResult pr = r.getPoly("class");
      QueryParser qp;

      if (pr.name.equals("classic")) {
        FieldDef fd = state.getField(pr.r, "defaultField");
        qp = new QueryParser(state.matchVersion, field == null ? fd.name : field, state.searchAnalyzer);

      } else if (pr.name.equals("MultiFieldQueryParser")) {
        List<Object> l = pr.r.getList("fields");
        String[] fields = new String[l.size()];
        Map<String,Float> boosts = new HashMap<String,Float>();
        for(int i=0;i<fields.length;i++) {
          Object o = l.get(i);
          String field2;
          float boost;

          if (o instanceof String) {
            field2 = (String) o;
            boost = 1.0f;
          } else {
            Request r2 = (Request) o;
            field2 = r2.getString("field");
            boost = r2.getFloat("boost");
          }

          FieldDef fd;
          try {
            fd = state.getField(field2);
          } catch (IllegalArgumentException iae) {
            pr.r.fail("fields", iae.toString());
            // Dead code but compiler disagrees:
            fd = null;
          }
          if (!fd.fieldType.indexed()) {
            pr.r.fail("fields", "field \"" + field2 + "\" was not registered with index=true");
          }
          fields[i] = field2;
          if (boost != 1.0f) {
            boosts.put(field, boost);
          }
        }

        qp = new MultiFieldQueryParser(state.matchVersion, fields, state.searchAnalyzer, boosts);
      } else {
        // BUG
        assert false;
        qp = null;
      }

      String opString = r.getEnum("defaultOperator");
      if (opString.equals("or")) {
        qp.setDefaultOperator(QueryParser.Operator.OR);
      } else {
        qp.setDefaultOperator(QueryParser.Operator.AND);
      }
      qp.setFuzzyMinSim(r.getInt("fuzzyMinSim"));
      qp.setFuzzyPrefixLength(r.getInt("fuzzyPrefixLength"));
      qp.setPhraseSlop(r.getInt("phraseSlop"));
      qp.setEnablePositionIncrements(r.getBoolean("enablePositionIncrements"));
      qp.setLowercaseExpandedTerms(r.getBoolean("lowercaseExpandedTerms"));
      if (r.hasParam("locale")) {
        qp.setLocale(getLocale(r.getStruct("locale")));
      }
      return qp;
    } else {
      List<String> fields;
      if (field != null) {
        fields = Collections.singletonList(field);
      } else {
        // Default to MultiFieldQueryParser over all indexed fields:
        fields = state.getIndexedAnalyzedFields();
      }
      return new MultiFieldQueryParser(state.matchVersion, fields.toArray(new String[fields.size()]), state.searchAnalyzer);
    }
  }

  /** Highlight configuration. */
  static class FieldHighlightConfig {
    /** Number of passages. */
    public int maxPassages = -1;

    // nocommit use enum:
    /** Snippet or whole. */
    public String mode;

    /** True if field is single valued. */
    public boolean multiValued;

    /** {@link BreakIterator} to use. */
    public BreakIterator breakIterator;
  }

  /** Retrieve the {@link SearcherAndTaxonomy} by version or
   *  snapshot. */
  public static SearcherAndTaxonomy getSearcherAndTaxonomy(Request request, IndexState state, long version,
                                                           IndexState.Gens snapshot, JSONObject diagnostics) throws IOException {
    SearcherAndTaxonomy s;

    if (version == -1) {
      // Request didn't specify any specific searcher;
      // just use the current (latest) searcher:
      s = state.manager.acquire();
      state.slm.record(s.searcher);
    } else {
      // Request specified a specific searcher by version:

      // nocommit need to generify this so we can pull
      // TaxoReader too:
      IndexSearcher priorSearcher = state.slm.acquire(version);
      if (priorSearcher == null) {
        if (snapshot != null) {
          // First time this snapshot is being searched
          // against (and the call to createSnapshot
          // didn't specify openSearcher=true); open the
          // reader:

          // TODO: this "reverse-NRT" is somewhat silly
          // ... Lucene needs a reader pool somehow:
          SearcherAndTaxonomy s2 = state.manager.acquire();
          try {
            // This returns a new reference to us, which
            // is decRef'd in the finally clause after
            // search is done:
            long t0 = System.nanoTime();
            IndexReader r = DirectoryReader.openIfChanged((DirectoryReader) s2.searcher.getIndexReader(),
                                                          state.snapshots.getIndexCommit(snapshot.indexGen));
            // nocommit messy:
            s2.taxonomyReader.incRef();
            s = new SearcherAndTaxonomy(new MyIndexSearcher(r), s2.taxonomyReader);
            state.slm.record(s.searcher);
            long t1 = System.nanoTime();
            if (diagnostics != null) {
              diagnostics.put("newSnapshotSearcherOpenMS", ((t1-t0)/1000000.0));
            }
          } finally {
            state.manager.release(s2);
          }
        } else {
          // Specific searcher version was requested,
          // but this searcher has timed out.  App
          // should present a "your session expired" to
          // user:
          request.fail("searcher", "This searcher has expired.");

          // Dead code but compiler disagrees:
          s = null;
        }
      } else {
        // nocommit messy ... we pull an old searcher
        // but the latest taxoReader ... necessary
        // because SLM can't take taxo reader yet:
        SearcherAndTaxonomy s2 = state.manager.acquire();
        s = new SearcherAndTaxonomy(priorSearcher, s2.taxonomyReader);
        s2.searcher.getIndexReader().decRef();
      }
    }

    return s;
  }

  @SuppressWarnings("unchecked")
  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    state.verifyStarted(r);

    final Map<ToParentBlockJoinQuery,BlockJoinQueryChild> useBlockJoinCollector = new HashMap<ToParentBlockJoinQuery,BlockJoinQueryChild>();

    final long timeStamp;
    if (r.hasParam("timeStamp")) {
      timeStamp = r.getLong("timeStamp");
    } else {
      timeStamp = System.currentTimeMillis()/1000;
    }

    String queryText;
    final Query q;
    if (r.hasParam("queryText")) {
      QueryParser queryParser = createQueryParser(state, r, null);

      queryText = r.getString("queryText");

      if (queryText != null) {
        try {
          q = queryParser.parse(queryText);
        } catch (Exception e) {
          r.fail("queryText", "could not parse", e);
          // dead code but compiler disagrees:
          return null;
        }
      } else {
        q = null;
      }
    } else if (r.hasParam("query")) {
      q = parseQuery(timeStamp, r, state, r.getStruct("query"), null, useBlockJoinCollector);
    } else {
      q = new MatchAllDocsQuery();
    }

    final JSONObject diagnostics = new JSONObject();

    // Figure out which searcher to use:
    final long searcherVersion;
    final IndexState.Gens searcherSnapshot;
    if (r.hasParam("searcher")) {
      Request r2 = r.getStruct("searcher");
      if (r2.hasParam("indexGen")) {
        long indexGen = r2.getLong("indexGen");
        long t0 = System.nanoTime();
        state.reopenThread.waitForGeneration(indexGen);
        diagnostics.put("nrtWaitMS", (System.nanoTime() - t0)/1000000);
        searcherVersion = -1;
        searcherSnapshot = null;
      } else if (r2.hasParam("version")) {
        searcherVersion = r2.getLong("version");
        searcherSnapshot = null;
      } else if (r2.hasParam("snapshot")) {
        searcherSnapshot = new IndexState.Gens(r2, "snapshot");
        Long v = state.snapshotGenToVersion.get(searcherSnapshot.indexGen);
        if (v == null) {
          r2.fail("snapshot", "unrecognized snapshot \"" + searcherSnapshot.id + "\"");
        }
        searcherVersion = v.longValue();
      } else {
        r.fail("searcher", "must specify exactly one of indexGen, version or snapshot");
        // Dead code but compiler disagrees:
        searcherSnapshot = null;
        searcherVersion = -1;
      }
    } else {
      searcherSnapshot = null;
      searcherVersion = -1;
    }

    final int topHits = r.getInt("topHits");
    final int startHit = r.getInt("startHit");

    final Sort sort;
    final boolean doMaxScore;
    final boolean doDocScores;

    if (r.hasParam("sort")) {
      // Sort by fields:
      Request sr = r.getStruct("sort");

      sort = parseSort(timeStamp, state, sr.getList("fields"));

      //System.out.println("sort=" + sort);
      doMaxScore = sr.getBoolean("doMaxScore");
      doDocScores = sr.getBoolean("doDocScores");
    } else {
      sort = null;
      doMaxScore = false;
      doDocScores = false;
    }

    final Filter filter;
    if (r.hasParam("filter")) {
      filter = parseFilter(timeStamp, r, state, r.getStruct("filter"));
    } else {
      filter = null;
    }

    final FacetSearchParams fsp;

    final Set<String> facetRequestDims;

    if (r.hasParam("facets")) {
      List<FacetRequest> requests = new ArrayList<FacetRequest>();
      facetRequestDims = new HashSet<String>();
      for(Object o2 : r.getList("facets")) {
        Request r2 = (Request) o2;

        if (r2.hasParam("numericRanges")) {
          FieldDef fd = state.getField(r2, "path");
          String path = fd.name;

          if (!fd.faceted.equals("numericRange")) {
            r2.fail("numericRanges", "field \"" + path + "\" was not registered with facet=numericRange");
          }
          if (fd.valueType.equals("int") || fd.valueType.equals("long")) {
            List<LongRange> ranges = new ArrayList<LongRange>();
            for(Object o : r2.getList("numericRanges")) {
              Request r3 = (Request) o;
              ranges.add(new LongRange(r3.getString("label"),
                                       r3.getLong("min"),
                                       r3.getBoolean("minInclusive"),
                                       r3.getLong("max"),
                                       r3.getBoolean("maxInclusive")));
            }
            requests.add(new RangeFacetRequest<LongRange>(path, ranges));
          } else {
            r2.fail("numericRanges", "only int/long currently supported");
          }
        } else {
          CategoryPath cp;
          if (r2.isString("path")) {
            String path = r2.getString("path");
            cp = new CategoryPath(path);
          } else {
            List<Object> l = r2.getList("path");
            if (l.isEmpty()) {
              r2.fail("path", "path must contain at least one part");
            }
            String[] path = new String[l.size()];
            for(int idx=0;idx<path.length;idx++) {
              path[idx] = l.get(idx).toString();
            }
            cp = new CategoryPath(path);
          }
          FieldDef fd = state.getField(cp.components[0]);
          if (fd.faceted.equals("no")) {
            r2.fail("path", "field \"" + fd.name + "\" was not registered with facet enabled");
          } else if (fd.faceted.equals("numericRange")) {
            r2.fail("path", "field \"" + fd.name + "\" was registered with facet=numericRange; must pass numericRanges in the request");
          }

          //System.out.println("cp: " + cp + "; len=" + cp.components.length);

          FacetRequest fr;
          if (r2.getBoolean("useOrdsCache") == false) {
            fr = new CountFacetRequest(cp, r2.getInt("topN"));
          } else {
            fr = new CountFacetRequest(cp, r2.getInt("topN")) {
              @Override
              public FacetsAggregator createFacetsAggregator(FacetIndexingParams fip) {
                return new CachedOrdsCountingFacetsAggregator();
              }
            };
          }
          requests.add(fr);

          facetRequestDims.add(cp.components[0]);
        }
      }
      fsp = requests.size() == 0 ? null : new FacetSearchParams(state.facetIndexingParams, requests);
    } else {
      fsp = null;
      facetRequestDims = null;
    }

    final Set<String> fields;
    final Map<String,FieldHighlightConfig> highlightFields;

    if (r.hasParam("retrieveFields")) {
      fields = new HashSet<String>();
      highlightFields = new HashMap<String,FieldHighlightConfig>();
      Set<String> fieldSeen = new HashSet<String>();
      for(Object o : r.getList("retrieveFields")) {
        String field;
        String highlight = "no";
        FieldHighlightConfig perField = null;
        if (o instanceof String) {
          field = (String) o;
          fields.add(field);
        } else if (o instanceof Request) {
          Request f = (Request) o;
          field = f.getString("field");
          if (f.hasParam("highlight")) {
            highlight = f.getEnum("highlight");
            if (!highlight.equals("no")) {
              perField = new FieldHighlightConfig();
              highlightFields.put(field, perField);
              perField.mode = highlight;
              if (f.hasParam("maxPassages")) {
                perField.maxPassages = f.getInt("maxPassages");
              }
              if (f.hasParam("breakIterator")) {
                perField.breakIterator = parseBreakIterator(f.getStruct("breakIterator"));
              }
            } else {
              fields.add(field);
            }
          } else {
            fields.add(field);
          }
        } else {
          r.fail("retrieveFields", "unrecognized object " + o);
          field = null;
        }
        if (fieldSeen.contains(field)) {
          r.fail("retrieveFields", "field \"" + field + "\" cannot be retrieved more than once");
        }       
        fieldSeen.add(field);
        FieldDef fd;
        try {
          fd = state.getField(field);
        } catch (IllegalArgumentException iae) {
          r.fail("retrieveFields", iae.toString());
          // Dead code but compiler disagrees:
          fd = null;
        }
        if (perField != null) {
          perField.multiValued = fd.multiValued;
          if (fd.multiValued == false && perField.mode.equals("joinedSnippets")) {
            ((Request) o).fail("highlight", "joinedSnippets can only be used with multi-valued fields");
          }
        }
        if (!highlight.equals("no") && !fd.highlighted) {
          r.fail("retrieveFields", "field \"" + field + "\" was not indexed with highlight=true");
        }
        if (!fd.fieldType.stored()) {
          // nocommit allow pulling from DV?  need separate dvFields?
          r.fail("retrieveFields", "field \"" + field + "\" was not registered with store=true");
        }
      }

    } else {
      fields = null;
      highlightFields = null;
    }

    final HighlighterConfig highlighter = getHighlighter(state, r, highlightFields);

    final String groupField;
    final Sort groupSort;
    final int groupsPerPage;
    final int hitsPerGroup;
    final int groupStart;
    final boolean doGroupMaxScore;
    final boolean doGroupDocScores;
    final boolean doTotalGroupCount;
    final DocValuesType groupDVType;

    if (r.hasParam("grouping")) {
      if (!useBlockJoinCollector.isEmpty()) {
        r.fail("grouping", "cannot do both grouping and ToParentBlockJoinQuery with returnChildHits=true");
      }
      Request grouping = r.getStruct("grouping");
      FieldDef fd = state.getField(grouping, "field");
      groupField = fd.name;
      groupDVType = fd.fieldType.docValueType();
      if (groupDVType == null) {
        grouping.fail("field", "field \"" + groupField + "\" was not registered with group=true");
      }

      if (grouping.hasParam("sort")) {
        groupSort = parseSort(timeStamp, state, grouping.getList("sort"));
      } else {
        groupSort = Sort.RELEVANCE;
      }
      groupsPerPage = grouping.getInt("groupsPerPage");
      hitsPerGroup = grouping.getInt("hitsPerGroup");
      doGroupMaxScore = grouping.getBoolean("doMaxScore");
      doGroupDocScores = grouping.getBoolean("doDocScores");
      doTotalGroupCount = grouping.getBoolean("doTotalGroupCount");
      groupStart = grouping.getInt("groupStart");
      //System.out.println("doGroupMaxScore=" + doGroupMaxScore);

    } else {
      groupField = null;
      groupSort = null;
      groupsPerPage = 0;
      hitsPerGroup = 0;
      doGroupMaxScore = false;
      doGroupDocScores = false;
      groupDVType = null;
      doTotalGroupCount = false;
      groupStart = 0;
    }

    final List<CategoryPath> allDrillDowns = new ArrayList<CategoryPath>();

    // True for pure browse ... we use this only for
    // browse-only facets caching:
    final boolean isMatchAll = q instanceof MatchAllDocsQuery && filter == null;

    final Set<String> drillDownDims;
    final List<CategoryPath[]> drillDowns;
    final List<NumericRangeQuery<? extends Number>> rangeDrillDowns;

    if (r.hasParam("drillDowns")) {
      List<Object> drillDownList = r.getList("drillDowns");
      if (!drillDownList.isEmpty()) {
        drillDowns = new ArrayList<CategoryPath[]>();
        rangeDrillDowns = new ArrayList<NumericRangeQuery<? extends Number>>();
        drillDownDims = new HashSet<String>();
        for(Object o : drillDownList) {
          Request fr = (Request) o;
          FieldDef fd = state.getField(fr, "field");
          String field = fd.name;

          List<Object> values = fr.getList("values");
          List<CategoryPath> paths = new ArrayList<CategoryPath>();
          for(Object o2 : values) {
            List<String> path = new ArrayList<String>();
            path.add(field);
            if (o2 instanceof JSONArray) {
              for(Object sub : ((JSONArray) o2)) {
                path.add(sub.toString());
              }
            } else {
              path.add(o2.toString());
            }
            if (fd.faceted.equals("numericRange")) {
              if (values.size() != 1) {
                fr.fail("values", "numericRange drilldown should have only one value");
              }
              if (path.size() != 2) {
                fr.fail("values", "numericRange drilldown should have only one value");
              }
              boolean found = false;
              for(FacetRequest request : fsp.facetRequests) {
                if (request.categoryPath.components[0].equals(field)) {
                  if (!(request instanceof RangeFacetRequest)) {
                    r.fail("facets", "field \"" + field + "\" must have numericRange facet request");
                  }
                  RangeFacetRequest<? extends Range> rfr = (RangeFacetRequest<? extends Range>) request;
                  for(Range range : rfr.ranges) {
                    if (range.label.equals(path.get(1))) {
                      if (range instanceof LongRange) {
                        LongRange lr = (LongRange) range;
                        rangeDrillDowns.add(NumericRangeQuery.newLongRange(path.get(0), lr.min, lr.max, lr.minInclusive, lr.maxInclusive));
                        found = true;
                        break;
                      } else {
                        throw new AssertionError("only Long ranges handled currently");
                      }
                    }
                  }
                }
              }
              if (!found) {
                fr.fail("field", "could not locate numeric range for \"" + path.get(1) + "\"");
              }
            } else {
              CategoryPath cp = new CategoryPath(path.toArray(new String[path.size()]));
              allDrillDowns.add(cp);
              paths.add(cp);
            }
          }
          if (!paths.isEmpty()) {
            drillDowns.add(paths.toArray(new CategoryPath[paths.size()]));
            drillDownDims.add(field);
          }
        }
      } else {
        drillDownDims = null;
        drillDowns = null;
        rangeDrillDowns = null;
      }
    } else {
      drillDownDims = null;
      drillDowns = null;
      rangeDrillDowns = null;
    }

    diagnostics.put("parsedQuery", q.toString());

    final int lastDocID;
    final float lastScore;
    final List<Object> lastFieldValues;
    if (r.hasParam("searchAfter")) {
      if (groupField != null) {
        r.fail("searchAfter", "cannot use searchAfter with grouping");
      }
      if (!useBlockJoinCollector.isEmpty()) {
        r.fail("searchAfter", "cannot use searchAfter with ToParentBlockJoinQuery with returnChildHits=true");
      }

      Request sa = r.getStruct("searchAfter");
      if (sa.hasParam("lastDoc")) {
        lastDocID = sa.getInt("lastDoc");
        if (sort != null) {
          lastFieldValues = sa.getList("lastFieldValues");
          if (lastFieldValues.size() != sort.getSort().length) {
            sa.fail("lastFieldValues", "length of lastFieldValues must be the same as number of sort fields; be sure to pass the previous page's searchState.lastFieldValues");
          }
          lastScore = 0.0f;
        } else {
          lastFieldValues = null;
          lastScore = sa.getFloat("lastScore");
        }
      } else {
        lastDocID = -1;
        lastScore = 0.0f;
        lastFieldValues = null;
      }
    } else {
      lastDocID = -1;
      lastScore = 0.0f;
      lastFieldValues = null;
    }

    return new FinishRequest() {

      @Override
      public String finish() throws IOException {
        // nocommit allow passing in prior search state &
        // using prior searcher

        List<FacetResult> facets;
        TopDocs hits;
        TopGroups<BytesRef> groups;
        TopGroups<Integer> joinGroups;
        int totalGroupCount = -1;
        final Map<String,FacetArrays> dimFacetArrays = new HashMap<String,FacetArrays>();

        // Pull the searcher we will use
        final SearcherAndTaxonomy s = getSearcherAndTaxonomy(r, state, searcherVersion, searcherSnapshot, diagnostics);
        long searcherToken = ((DirectoryReader) s.searcher.getIndexReader()).getVersion();

        try {

          Query q2 = s.searcher.rewrite(q);
          //System.out.println("after rewrite: " + q2);
          diagnostics.put("rewrittenQuery", q2.toString());

          if (filter != null) {
            q2 = new FilteredQuery(q2, filter);
          }

          // TODO: re-enable this?  else we never get
          // in-order collectors
          //Weight w = s.createNormalizedWeight(q2);

          // If there are any drill-downs, wrap in
          // DrillDownQuery:
          DrillDownQuery ddq = new DrillDownQuery(state.facetIndexingParams, q2);
          if (drillDowns != null) {
            for(CategoryPath[] dd : drillDowns) {
              ddq.add(dd);
            }
          }
          if (rangeDrillDowns != null) {
            for(NumericRangeQuery nrq : rangeDrillDowns) {
              ddq.add(nrq.getField(), nrq);
            }
          }
          q2 = ddq;
          diagnostics.put("drillDownQuery", q2.toString());

          Collector c;
          TermFirstPassGroupingCollector groupCollector = null;
          TermAllGroupsCollector allGroupsCollector = null;
          if (groupField != null) {
            groupCollector = new TermFirstPassGroupingCollector(groupField, groupSort, groupsPerPage);
            if (doTotalGroupCount) {
              allGroupsCollector = new TermAllGroupsCollector(groupField);
              c = MultiCollector.wrap(groupCollector, allGroupsCollector);
            } else {
              c = groupCollector;
            }
          } else if (!useBlockJoinCollector.isEmpty()) {
            Iterator<Map.Entry<ToParentBlockJoinQuery,BlockJoinQueryChild>> it = useBlockJoinCollector.entrySet().iterator();
            Map.Entry<ToParentBlockJoinQuery,BlockJoinQueryChild> ent = it.next();
            BlockJoinQueryChild child = ent.getValue();
            c = new ToParentBlockJoinCollector(sort == null ? Sort.RELEVANCE : sort,
                                               topHits, child.trackScores, child.trackMaxScore);
          } else if (sort == null) {
            ScoreDoc searchAfter;
            if (lastDocID != -1) {
              searchAfter = new ScoreDoc(lastDocID, lastScore);
            } else {
              searchAfter = null;
            }
            //c = TopScoreDocCollector.create(topHits, searchAfter, !w.scoresDocsOutOfOrder());
            c = TopScoreDocCollector.create(topHits, searchAfter, false);
          } else {
            FieldDoc searchAfter;
            if (lastDocID != -1) {
              searchAfter = new FieldDoc(lastDocID, lastScore, lastFieldValues.toArray(new Object[lastFieldValues.size()]));
            } else {
              searchAfter = null;
            }
            //c = TopFieldCollector.create(sort, topHits, searchAfter, true, doDocScores, doMaxScore, !w.scoresDocsOutOfOrder());
            c = TopFieldCollector.create(sort, topHits, searchAfter, true, doDocScores, doMaxScore, false);
          }

          DrillSideways.DrillSidewaysResult dsResults;

          // Careful top-level facets cache logic:
          Map<FacetRequest,FacetResult> cachedFacetResults = null;

          // nocommit can we do better?  sometimes downgrade
          // to DDQ not DS?
          // nocommit improve this: we can use the cache if
          // there's a single DS dim, for just that dim
          // (other dims must recompute)
          // nocommit turn this back on but ... this causes
          // NPEs when drillDownFacet tries to fill

          FacetSearchParams fsp2;

          JSONArray cachedDims = new JSONArray();
          diagnostics.put("facetCachedDims", cachedDims);
          //System.out.println("isMatchAll=" + isMatchAll);

          if (isMatchAll && fsp != null && (drillDowns == null || drillDowns.size() == 1)) {
            TopFacetsCache cache = ((MyIndexSearcher) s.searcher).topFacetsCache;
            cachedFacetResults = new HashMap<FacetRequest,FacetResult>();

            List<FacetRequest> unCachedRequests = new ArrayList<FacetRequest>();

            for (FacetRequest req : fsp.facetRequests) {
              if (req instanceof RangeFacetRequest) {
                unCachedRequests.add(req);
                continue;
              }
              String dim = req.categoryPath.components[0];
              boolean doCache = false;
              if (drillDowns == null || drillDownDims.contains(dim)) {
                // This dimension is the "browse only" facet
                // counts, so we can consult cache:
                FacetResult result = cache.get(req);
                cachedDims.add(dim);
                if (result != null) {
                  Set<Integer> ords = new HashSet<Integer>();
                  for(FacetResultNode childNode : result.getFacetResultNode().subResults) {
                    ords.add(childNode.ordinal);
                  }
                  doCache = true;
                  for(CategoryPath cp : allDrillDowns) {
                    if (cp.components[0].equals(dim)) {
                      int ord = s.taxonomyReader.getOrdinal(cp);
                      if (ord == -1) {
                        r.fail("drillDowns", "path " + cp + " does not exist");
                      }
                      if (!ords.contains(ord)) {
                        // Cannot use cache: there is an
                        // explicit drill down on this dim
                        // that was not counted in the
                        // cached results:
                        doCache = false;
                        break;
                      }
                    }
                  }
                  if (doCache) {
                    cachedFacetResults.put(req, result);
                  }
                }
              }
              if (!doCache) {
                unCachedRequests.add(req);
              }
            }

            if (!cachedFacetResults.isEmpty()) {
              // Make a new FacetSearchParams:
              if (unCachedRequests.isEmpty()) {
                fsp2 = null;
              } else {
                fsp2 = new FacetSearchParams(state.facetIndexingParams, unCachedRequests);
              }
            } else {
              // No dimensions were cached:
              fsp2 = fsp;
            }
          } else {
            // Browse only facet cache does not apply
            // (e.g. because there are 2 drill downs, or
            // because query is not MatchAll):
            fsp2 = fsp;
          }

          //System.out.println("useCache=" + useFacetsCache + " cacheHit=" + facetsCacheHit);

          long searchStartTime = System.nanoTime();

          if (fsp2 != null) {
            // Use DrillSideways to do the search, so we get
            // sideways counts
            DrillSideways ds = new DrillSideways(s.searcher, s.taxonomyReader) {
                @Override
                protected FacetsAccumulator getDrillDownAccumulator(FacetSearchParams fsp) {
                  FacetArrays arrays = new FacetArrays(s.taxonomyReader.getSize());
                  dimFacetArrays.put(null, arrays);
                  // nocommit: cutover to lucene's
                  return MultiFacetsAccumulator.create(fsp, searcher.getIndexReader(), s.taxonomyReader, arrays);
                }

                @Override
                protected FacetsAccumulator getDrillSidewaysAccumulator(String dim, FacetSearchParams fsp) {
                  FacetArrays arrays = new FacetArrays(s.taxonomyReader.getSize());
                  dimFacetArrays.put(dim, arrays);
                  // nocommit: cutover to lucene's
                  return MultiFacetsAccumulator.create(fsp, searcher.getIndexReader(), s.taxonomyReader, arrays);
                }

                @Override
                protected boolean scoreSubDocsAtOnce() {
                  // If we are using
                  // ToParentBlockJoinCollector then all
                  // sub-docs must be scored at once:
                  return !useBlockJoinCollector.isEmpty();
                }
              };

            dsResults = ds.search((DrillDownQuery) q2, c, fsp2);
          } else {
            //((MyIndexSearcher) s).search(w, c2);
            s.searcher.search(q2, c);
            dsResults = null;
          }
          diagnostics.put("firstPassSearchMS", ((System.nanoTime()-searchStartTime)/1000000.0));

          if (groupField != null) {
            Collection<SearchGroup<BytesRef>> topGroups = groupCollector.getTopGroups(groupStart, true);
            if (topGroups != null) {
              TermSecondPassGroupingCollector c3 = new TermSecondPassGroupingCollector(groupField,
                                                                                       topGroups,
                                                                                       groupSort,
                                                                                       sort,
                                                                                       hitsPerGroup,
                                                                                       doGroupDocScores,
                                                                                       doGroupMaxScore,
                                                                                       true);
              long t0 = System.nanoTime();
              // TODO: should we ... pre-rewrite a query if
              // we know we will need to do 2 passes?
              //((MyIndexSearcher) s).search(w, c3);
              s.searcher.search(q2, c3);
              diagnostics.put("secondPassSearchMS", ((System.nanoTime()-t0)/1000000));

              groups = c3.getTopGroups(0);
              hits = null;
              joinGroups = null;
              if (allGroupsCollector != null) {
                totalGroupCount = allGroupsCollector.getGroups().size();
              }
            } else {
              hits = null;
              groups = null;
              joinGroups = null;
              totalGroupCount = 0;
            }
          } else if (!useBlockJoinCollector.isEmpty()) {
            assert useBlockJoinCollector.size() == 1;
            Iterator<Map.Entry<ToParentBlockJoinQuery,BlockJoinQueryChild>> it = useBlockJoinCollector.entrySet().iterator();
            Map.Entry<ToParentBlockJoinQuery,BlockJoinQueryChild> ent = it.next();
            BlockJoinQueryChild child = ent.getValue();

            joinGroups = ((ToParentBlockJoinCollector) c).getTopGroups(ent.getKey(),
                                                                       child.sort, startHit,
                                                                       child.maxChildren, 0, true);
            groups = null;
            hits = null;
          } else {
            groups = null;
            joinGroups = null;
            hits = ((TopDocsCollector) c).topDocs();

            if (startHit != 0) {
              // Slice:
              int count = Math.max(0, hits.scoreDocs.length - startHit);
              ScoreDoc[] newScoreDocs = new ScoreDoc[count];
              if (count > 0) {
                System.arraycopy(hits.scoreDocs, startHit, newScoreDocs, 0, count);
              }
              hits = new TopDocs(hits.totalHits,
                                 newScoreDocs,
                                 hits.getMaxScore());
            }
          }

          if (fsp != null) {
            if (dsResults != null) {
              facets = dsResults.facetResults;
            } else {
              facets = null;
            }

            // nocommit disabled top facets cache for testing
            if (false && isMatchAll && drillDowns == null && facets != null) {
              // Only cache on pure browse case, and don't
              // cache dynamic numeric ranges:
              TopFacetsCache cache = ((MyIndexSearcher) s.searcher).topFacetsCache;
              for(FacetResult fr : facets) {
                if (!(fr.getFacetRequest() instanceof RangeFacetRequest) && fr.getFacetRequest().numResults < TOP_FACET_CACHE_MAX_FACET_COUNT) {
                  cache.add(fr.getFacetRequest(), fr);
                }
              }
            }
          } else {
            facets = null;
          }

          int[] highlightDocIDs = null;
          if (groupField != null) {
            if (groups != null) {

              // These groups are already sliced according
              // to groupStart:
              int count = 0;
              for(GroupDocs<BytesRef> group : groups.groups) {
                count += group.scoreDocs.length;
              }
              if (count > 0) {
                highlightDocIDs = new int[count];

                int upto = 0;
                for(GroupDocs<BytesRef> group : groups.groups) {
                  for(ScoreDoc scoreDoc : group.scoreDocs) {
                    highlightDocIDs[upto++] = scoreDoc.doc;
                  }
                }
              }
            }
          } else if (!useBlockJoinCollector.isEmpty()) {
            if (joinGroups != null) {

              int count = 0;
              for(GroupDocs<Integer> group : joinGroups.groups) {
                // for the parent docID:
                count++;
                // for all child docs:
                count += group.scoreDocs.length;
              }

              if (count > 0) {
                highlightDocIDs = new int[count];

                int upto = 0;
                for(GroupDocs<Integer> group : joinGroups.groups) {
                  highlightDocIDs[upto++] = group.groupValue.intValue();
                  for(ScoreDoc scoreDoc : group.scoreDocs) {
                    highlightDocIDs[upto++] = scoreDoc.doc;
                  }
                }
              }
            }
          } else {
            highlightDocIDs = new int[hits.scoreDocs.length];
            for(int i=0;i<hits.scoreDocs.length;i++) {
              highlightDocIDs[i] = hits.scoreDocs[i].doc;
            }
          }

          Map<String,String[]> highlights = null;

          long t0 = System.nanoTime();
          if (highlightDocIDs != null && highlightFields != null && !highlightFields.isEmpty()) {
            int[] maxPassages = new int[highlightFields.size()];
            Arrays.fill(maxPassages, highlighter.maxPassages);
            String[] fields = new String[highlightFields.size()];
            int upto = 0;
            for(Map.Entry<String,FieldHighlightConfig> ent : highlightFields.entrySet()) {
              fields[upto] = ent.getKey();
              FieldHighlightConfig perField = ent.getValue();
              if (perField.maxPassages != -1) {
                maxPassages[upto] = perField.maxPassages;
              }
              upto++;
            }

            highlights = highlighter.highlighter.highlightFields(fields,
                                                                 q,
                                                                 s.searcher,
                                                                 highlightDocIDs,
                                                                 maxPassages);
          }
          diagnostics.put("highlightTimeMS", (System.nanoTime() - t0)/1000000.);

          JSONObject o = new JSONObject();
          o.put("diagnostics", diagnostics);
          t0 = System.nanoTime();
        
          if (groupField != null) {
            if (groups == null) {
              o.put("totalHits", 0);
              o.put("totalGroupCount", 0);
            } else {
              o.put("totalHits", groups.totalHitCount);
              o.put("totalGroupedHits", groups.totalGroupedHitCount);
              if (groups.totalGroupCount != null) {
                o.put("totalGroupCount", groups.totalGroupCount);
              } else if (totalGroupCount != -1) {
                o.put("totalGroupCount", totalGroupCount);
              }

              // nocommit why am I getting a maxScore back when
              // I didn't ask for it ... oh because I'm sorting
              // by relevance ... hmm ... must test field sort
              // case

              if (!Float.isNaN(groups.maxScore)) {
                o.put("maxScore", groups.maxScore);
              }

              JSONArray o2 = new JSONArray();
              o.put("groups", o2);
              int hitIndex = 0;
              for(GroupDocs<BytesRef> group : groups.groups) {
                JSONObject o3 = new JSONObject();
                o2.add(o3);
                Object v = group.groupValue;
                if (v instanceof BytesRef) {
                  o3.put("groupValue", ((BytesRef) v).utf8ToString());
                } else {
                  o3.put("groupValue", v);
                }
                o3.put("totalHits", group.totalHits);

                if (!Float.isNaN(group.maxScore)) {
                  o3.put("maxScore", group.maxScore);
                }

                if (!Float.isNaN(group.score)) {
                  o3.put("score", group.score);
                }

                JSONObject o4 = new JSONObject();
                o3.put("groupSortFields", o4);
                SortField[] groupSortFields = groupSort.getSort();
                for(int i=0;i<groupSortFields.length;i++) {
                  String field = groupSortFields[i].getField();
                  if (field == null) {
                    field = "<score>";
                  }
                  o4.put(field, group.groupSortValues[i]);
                }

                JSONArray o5 = new JSONArray();
                o3.put("hits", o5);

                for(ScoreDoc hit : group.scoreDocs) {
                  JSONObject o6 = new JSONObject();
                  o5.add(o6);
                  o6.put("doc", hit.doc);
                  if (!Float.isNaN(hit.score)) {
                    o6.put("score", hit.score);
                  }

                  if (fields != null || highlightFields != null) {
                    JSONObject o7 = new JSONObject();
                    o6.put("fields", o7);
                    fillFields(state, highlighter, s.searcher, o7, hit, fields, highlights, hitIndex, sort);
                  }

                  hitIndex++;
                }
              }
            }
          } else if (!useBlockJoinCollector.isEmpty()) {
            // ToParentBlockJoin
            if (joinGroups == null) {
              o.put("totalHits", 0);
              o.put("totalGroupCount", 0);
            } else {

              assert useBlockJoinCollector.size() == 1;
              Iterator<Map.Entry<ToParentBlockJoinQuery,BlockJoinQueryChild>> it = useBlockJoinCollector.entrySet().iterator();
              Map.Entry<ToParentBlockJoinQuery,BlockJoinQueryChild> ent = it.next();
              BlockJoinQueryChild child = ent.getValue();

              o.put("totalHits", joinGroups.totalHitCount);
              o.put("totalGroupedHits", joinGroups.totalGroupedHitCount);
              if (joinGroups.totalGroupCount != null) {
                o.put("totalGroupCount", joinGroups.totalGroupCount);
              }

              // nocommit why am I getting a maxScore back when
              // I didn't ask for it ... oh because I'm sorting
              // by relevance ... hmm ... must test field sort
              // case

              if (!Float.isNaN(joinGroups.maxScore)) {
                o.put("maxScore", joinGroups.maxScore);
              }

              JSONArray o2 = new JSONArray();
              o.put("groups", o2);
              int hitIndex = 0;
              for(GroupDocs<Integer> group : joinGroups.groups) {
                JSONObject o3 = new JSONObject();
                o2.add(o3);
                if (fields != null || highlightFields != null) {
                  JSONObject o4 = new JSONObject();
                  o3.put("fields", o4);
                  // nocommit where does parent score come
                  // from ...
                  ScoreDoc sd = new ScoreDoc(group.groupValue.intValue(), 0.0f);
                  fillFields(state, highlighter, s.searcher, o4, sd, fields, highlights, hitIndex, sort);
                }
                hitIndex++;

                o3.put("totalHits", group.totalHits);

                if (!Float.isNaN(group.maxScore)) {
                  o3.put("maxScore", group.maxScore);
                }

                JSONObject o4 = new JSONObject();
                o3.put("groupSortFields", o4);
                SortField[] groupSortFields = (child.sort == null ? Sort.RELEVANCE : child.sort).getSort();
                for(int i=0;i<groupSortFields.length;i++) {
                  String field = groupSortFields[i].getField();
                  if (field == null) {
                    field = "<score>";
                  }
                  o4.put(field, group.groupSortValues[i]);
                }

                JSONArray o5 = new JSONArray();
                o3.put("hits", o5);

                for(ScoreDoc hit : group.scoreDocs) {
                  JSONObject o6 = new JSONObject();
                  o5.add(o6);
                  o6.put("doc", hit.doc);
                  if (!Float.isNaN(hit.score)) {
                    o6.put("score", hit.score);
                  }

                  if (fields != null || highlightFields != null) {
                    JSONObject o7 = new JSONObject();
                    o6.put("fields", o7);
                    fillFields(state, highlighter, s.searcher, o7, hit, fields, highlights, hitIndex, child.sort);
                  }

                  hitIndex++;
                }
              }
            }

          } else {
            o.put("totalHits", hits.totalHits);
            JSONArray o2 = new JSONArray();
            o.put("hits", o2);
            if (!Float.isNaN(hits.getMaxScore())) {
              o.put("maxScore", hits.getMaxScore());
            }

            for(int hitIndex=0;hitIndex<hits.scoreDocs.length;hitIndex++) {
              ScoreDoc hit = hits.scoreDocs[hitIndex];

              JSONObject o3 = new JSONObject();
              o2.add(o3);
              o3.put("doc", hit.doc);
              if (!Float.isNaN(hit.score)) {
                o3.put("score", hit.score);
              }

              if (fields != null || highlightFields != null) {
                JSONObject o4 = new JSONObject();
                o3.put("fields", o4);
                fillFields(state, highlighter, s.searcher, o4, hit, fields, highlights, hitIndex, sort);
              }
            }
          }

          JSONObject o3 = new JSONObject();
          o.put("searchState", o3);
          o3.put("timeStamp", timeStamp);

          o3.put("searcher", searcherToken);
          if (hits != null && hits.scoreDocs.length != 0) {
            ScoreDoc lastHit = hits.scoreDocs[hits.scoreDocs.length-1];
            o3.put("lastDoc", lastHit.doc);
            if (sort != null) {
              JSONArray fieldValues = new JSONArray();
              o3.put("lastFieldValues", fieldValues);
              FieldDoc fd = (FieldDoc) lastHit;
              for(Object fv : fd.fields) {
                fieldValues.add(fv);
              }
            } else {
              o3.put("lastScore", lastHit.score);
            }
          }

          if (fsp != null) {
            JSONArray o5 = new JSONArray();
            o.put("facets", o5);
            Set<Integer> seenOrds = new HashSet<Integer>();

            // So we can lookup FacetResult keyed by its
            // corresponding FacetRequest:
            Map<FacetRequest,FacetResult> byRequest = new HashMap<FacetRequest,FacetResult>();
            if (facets != null) {
              for(FacetResult fr : facets) {
                byRequest.put(fr.getFacetRequest(), fr);
              }
            }

            // Collate facet results, from either the cached
            // results or computed results:
            for(FacetRequest facetRequest : fsp.facetRequests) {
              FacetResult fr = byRequest.get(facetRequest);
              if (fr == null) {
                fr = cachedFacetResults.get(facetRequest);
                assert fr != null;
              }
              JSONObject fo = new JSONObject();
              o5.add(fo);
              JSONArray o6 = new JSONArray();
              fo.put("counts", o6);
              fo.put("numValidDescendants", fr.getNumValidDescendants());
              FacetResultNode n = fr.getFacetResultNode();
              JSONArray pair = new JSONArray();
              o6.add(pair);
              pair.add("top");
              pair.add((int) n.value);
              int childIDX = fr.getFacetRequest().categoryPath.components.length;
              for(FacetResultNode childNode : n.subResults) {
                pair = new JSONArray();
                o6.add(pair);
                pair.add(childNode.label.components[childIDX]);
                pair.add((int) childNode.value);
                seenOrds.add(childNode.ordinal);
              }
            }

            // Separately add facet count for any drill
            // downs, if those values were not already
            // included in the facet requests.  For example,
            // this can happen in the books UI if the user
            // drills down on the author of a particular
            // book but that author doesn't make the top 10:
            JSONArray extra = null;
            for(CategoryPath cp : allDrillDowns) {
              if (!facetRequestDims.contains(cp.components[0])) {
                continue;
              }
              int ord = s.taxonomyReader.getOrdinal(cp);
              if (ord == -1) {
                r.fail("drillDowns", "path " + cp + " does not exist");
              } else if (!seenOrds.contains(ord)) {
                if (extra == null) {
                  extra = new JSONArray();
                  o.put("drillDownFacets", extra);
                }
                FacetArrays array = dimFacetArrays.get(cp.components[0]);
                if (array == null) {
                  array = dimFacetArrays.get(null);
                }
                assert array != null;
                JSONArray facet = new JSONArray();
                extra.add(facet);
                JSONArray path = new JSONArray();
                facet.add(path);
                for(int i=0;i<cp.length;i++) {
                  path.add(cp.components[i]);
                }
                facet.add(array.getIntArray()[ord]);
              }
            }
          }

          diagnostics.put("getFieldsMS", ((System.nanoTime()-t0)/1000000));

          t0 = System.nanoTime();
          String ret = o.toString();
          //System.out.println("MS: " + ((System.nanoTime()-t0)/1000000.0));
          return ret;
        
        } finally {
          // NOTE: this is a little iffy, because we may not
          // have obtained this searcher from the NRTManager
          // (i.e. sometimes we pulled from
          // SearcherLifetimeManager, other times (if
          // snapshot was specified) we opened ourselves,
          // but under-the-hood all this method does is
          // s.getIndexReader().decRef(), which "works" for
          // all ways:
          state.manager.release(s);
        }
      }
    };
  }

  /** Parses the {@link Request} into a {@link Locale}. */
  public static Locale getLocale(Request r) {
    Locale locale;
    if (!r.hasParam("variant")) {
      if (!r.hasParam("country")) {
        locale = new Locale(r.getString("language"));
      } else {
        locale = new Locale(r.getString("language"),
                            r.getString("country"));
      }
    } else {
      locale = new Locale(r.getString("language"),
                          r.getString("country"),
                          r.getString("variant"));
    }

    return locale;
  }
}
