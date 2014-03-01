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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.search.suggest.DocumentDictionary;
import org.apache.lucene.search.suggest.DocumentValueSourceDictionary;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup.LookupResult; // javadocs
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.FuzzySuggester;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.FromFileTermFreqIterator;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import org.apache.lucene.server.params.PolyType.PolyEntry;
import org.apache.lucene.util.BytesRef;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;

import static org.apache.lucene.server.handlers.RegisterFieldHandler.ANALYZER_TYPE;

/** Handles {@code buildSuggest}. */
public class BuildSuggestHandler extends Handler {

  private final static StructType TYPE =
    new StructType(
        new Param("indexName", "Index Name", new StringType()),
        new Param("class", "Which suggester implementation to use",
            new PolyType(Lookup.class,
                         new PolyEntry("AnalyzingSuggester", "Suggester that first analyzes the surface form, adds the analyzed form to a weighted FST, and then does the same thing at lookup time (see @lucene:suggest:org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester)",
                             new Param("analyzer", "Index and query analyzer", ANALYZER_TYPE),
                             new Param("indexAnalyzer", "Index analyzer", ANALYZER_TYPE),
                             new Param("queryAnalyzer", "Query analyzer", ANALYZER_TYPE),
                             new Param("maxSurfaceFormsPerAnalyzedForm", "Maximum number of surface forms to keep for a single analyzed form", new IntType(), 256),
                             new Param("maxGraphExpansions", "Maximum number of graph paths to expand from the analyzed from", new IntType(), -1),
                             new Param("preserveSep", "True if token separators should be preserved when matching", new BooleanType(), true),
                             new Param("exactFirst", "True if the exact match should always be returned first regardless of score", new BooleanType(), true)),
                         new PolyEntry("FuzzySuggester", "Implements a fuzzy AnalyzingSuggester (see @lucene:suggest:org.apache.lucene.search.suggest.analyzing.FuzzySuggester)",
                             new Param("analyzer", "Index and query analyzer", ANALYZER_TYPE),
                             new Param("indexAnalyzer", "Index analyzer", ANALYZER_TYPE),
                             new Param("queryAnalyzer", "Query analyzer", ANALYZER_TYPE),  
                             new Param("maxSurfaceFormsPerAnalyzedForm", "Maximum number of surface forms to keep for a single analyzed form", new IntType(), 156),
                             new Param("maxGraphExpansions", "Maximum number of graph paths to expand from the analyzed from", new IntType(), -1),
                             new Param("preserveSep", "True if token separators should be preserved when matching", new BooleanType(), true),
                             new Param("exactFirst", "True if the exact match should always be returned first regardless of score", new BooleanType(), true),
                             new Param("minFuzzyLength", "Minimum key length before edits are allowed", new IntType(), FuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH),
                             new Param("nonFuzzyPrefix", "Key prefix where edits are not allowed", new IntType(), FuzzySuggester.DEFAULT_NON_FUZZY_PREFIX),
                             new Param("maxEdits", "Maximum number of edits for fuzzy suggestions", new IntType(), FuzzySuggester.DEFAULT_MAX_EDITS),
                             new Param("transpositions", "Whether transpositions are allowed", new BooleanType(), FuzzySuggester.DEFAULT_TRANSPOSITIONS),
                             new Param("unicodeAware", "True if all edits are measured in unicode characters, not UTF-8 bytes", new BooleanType(), FuzzySuggester.DEFAULT_UNICODE_AWARE)),
                         new PolyEntry("InfixSuggester", "A suggester that matches terms anywhere in the input text, not just as a prefix. (see @lucene:org:server.InfixSuggester)",
                             new Param("analyzer", "Index and query analyzer", ANALYZER_TYPE),
                             new Param("indexAnalyzer", "Index analyzer", ANALYZER_TYPE),
                             new Param("queryAnalyzer", "Query analyzer", ANALYZER_TYPE))),
                  "AnalyzingSuggester"),
        // nocommit option to stream suggestions in over the wire too
        new Param("source", "Where to get suggestions from",
            new StructType(
                  new Param("localFile", "Local file (to the server) to read suggestions + weights from; format is weight U+001F suggestion U+001F payload, one per line, with suggestion UTF-8 encoded.  If this option is used then searcher, suggestField, weightField/Expression, payloadField should not be specified.", new StringType()),
                  new Param("searcher", "Specific searcher version to use for pull suggestions to build.  There are three different ways to specify a searcher version.",
                            SearchHandler.SEARCHER_VERSION_TYPE),
                  new Param("suggestField", "Field (from stored documents) containing the suggestion text", new StringType()),
                  new Param("weightField", "Numeric field (from stored documents) containing the weight", new StringType()),
                  new Param("weightExpression", "Alternative to weightField, an expression that's evaluated to the weight.  Note that any fields referenced in the expression must have been indexed with sort=true.", new StringType()),
                  new Param("payloadField", "Optional binary or string field (from stored documents) containing the payload", new StringType()))),
        new Param("suggestName", "Unique name for this suggest build.", new StringType())
                   );

  /** Sole constructor. */
  public BuildSuggestHandler(GlobalState state) {
    super(state);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Builds a new auto-suggester, loading suggestions via the provided local file path.";
  }

  /** Load all previously built suggesters. */
  public void load(IndexState state, JSONObject saveState) throws IOException {
    for(Map.Entry<String,Object> ent : saveState.entrySet()) {
      String suggestName = ent.getKey();

      JSONObject params = (JSONObject) ent.getValue();
      String jsonOrig = params.toString();

      Request r = new Request(null, null, params, TYPE);
      // Must consume these up front since getSuggester
      // won't:
      r.getString("suggestName");
      Request source = r.getStruct("source");
      if (source.hasParam("localFile")) {
        source.getString("localFile");
      } else {
        Request searcher = source.getStruct("searcher");
        if (searcher.hasParam("indexGen")) {
          searcher.getLong("indexGen");
        } else {        
          searcher.getString("snapshot");
        }
        source.getString("suggestField");
        if (source.hasParam("weightField")) {
          source.getString("weightField");
        } else {
          source.getString("weightExpression");
        }
        if (source.hasParam("payloadField")) {
          source.getString("payloadField");
        }
      }
      Lookup suggester = getSuggester(state, suggestName, r);
      assert !Request.anythingLeft(params);

      if ((suggester instanceof AnalyzingInfixSuggester) == false) {
        File path = new File(state.rootDir, "suggest." + suggestName);
        FileInputStream in = new FileInputStream(path);
        try {
          suggester.load(in);
        } finally {
          in.close();
        }
      }

      try {
        state.addSuggest(suggestName, (JSONObject) JSONValue.parseStrict(jsonOrig));
      } catch (ParseException pe) {
        // BUG
        throw new RuntimeException(pe);
      }
    }
  }

  private Lookup getSuggester(IndexState state, String suggestName, Request r) throws IOException {

    Request.PolyResult pr = r.getPoly("class");
    
    String impl = pr.name;

    r = pr.r;
    Analyzer indexAnalyzer;
    Analyzer queryAnalyzer;
    // nocommit allow passing a field name, and we use that
    // field name's analyzer?
    if (r.hasParam("analyzer")) {
      indexAnalyzer = queryAnalyzer = RegisterFieldHandler.getAnalyzer(state, r, "analyzer");
    } else {
      indexAnalyzer = RegisterFieldHandler.getAnalyzer(state, r, "indexAnalyzer");
      queryAnalyzer = RegisterFieldHandler.getAnalyzer(state, r, "queryAnalyzer");
    }
    if (indexAnalyzer == null) {
      r.fail("analyzer", "analyzer or indexAnalyzer must be specified");
    }
    if (queryAnalyzer == null) {
      r.fail("analyzer", "analyzer or queryAnalyzer must be specified");
    }
    final Lookup suggester;

    if (impl.equals("FuzzySuggester")) {
      int options = 0;
      if (r.getBoolean("preserveSep")) {
        options |= AnalyzingSuggester.PRESERVE_SEP;
      }
      if (r.getBoolean("exactFirst")) {
        options |= AnalyzingSuggester.EXACT_FIRST;
      }
      suggester = new FuzzySuggester(indexAnalyzer, queryAnalyzer,
                                     options,
                                     r.getInt("maxSurfaceFormsPerAnalyzedForm"),
                                     r.getInt("maxGraphExpansions"),
                                     true,
                                     r.getInt("maxEdits"),
                                     r.getBoolean("transpositions"),
                                     r.getInt("nonFuzzyPrefix"),
                                     r.getInt("minFuzzyLength"),
                                     r.getBoolean("unicodeAware"));
    } else if (impl.equals("AnalyzingSuggester")) {
      int options = 0;
      if (r.getBoolean("preserveSep")) {
        options |= AnalyzingSuggester.PRESERVE_SEP;
      }
      if (r.getBoolean("exactFirst")) {
        options |= AnalyzingSuggester.EXACT_FIRST;
      }
      suggester = new AnalyzingSuggester(indexAnalyzer, queryAnalyzer, options,
                                         r.getInt("maxSurfaceFormsPerAnalyzedForm"),
                                         r.getInt("maxGraphExpansions"), true);
    } else if (impl.equals("InfixSuggester")) {
      suggester = new AnalyzingInfixSuggester(state.matchVersion,
                                              new File(state.rootDir, "suggest." + suggestName + ".infix"),
                                              indexAnalyzer,
                                              queryAnalyzer,
                                              AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS) {
          @Override
          protected Object highlight(String text, Set<String> matchedTokens, String prefixToken) throws IOException {
            
            // We override the entire highlight method, to
            // render directly to JSONArray instead of html
            // string:

            TokenStream ts = queryAnalyzer.tokenStream("text", new StringReader(text));
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            ts.reset();
            JSONArray fragments = new JSONArray();
            int upto = 0;
            while (ts.incrementToken()) {
              String token = termAtt.toString();
              int startOffset = offsetAtt.startOffset();
              int endOffset = offsetAtt.endOffset();
              if (upto < startOffset) {
                JSONObject o = new JSONObject();
                fragments.add(o);
                o.put("isHit", false);
                o.put("text", text.substring(upto, startOffset));
                upto = startOffset;
              } else if (upto > startOffset) {
                continue;
              }

              if (matchedTokens.contains(token)) {
                // Token matches.
                JSONObject o = new JSONObject();
                fragments.add(o);
                o.put("isHit", true);
                o.put("text", text.substring(startOffset, endOffset));
                upto = endOffset;
              } else if (prefixToken != null && token.startsWith(prefixToken)) {
                JSONObject o = new JSONObject();
                fragments.add(o);
                o.put("isHit", true);
                o.put("text", text.substring(startOffset, startOffset+prefixToken.length()));
                if (prefixToken.length() < token.length()) {
                  o = new JSONObject();
                  fragments.add(o);
                  o.put("isHit", false);
                  o.put("text", text.substring(startOffset+prefixToken.length(), startOffset+token.length()));
                }
                upto = endOffset;
              }
            }
            ts.end();
            int endOffset = offsetAtt.endOffset();
            if (upto < endOffset) {
              JSONObject o = new JSONObject();
              fragments.add(o);
              o.put("isHit", false);
              o.put("text", text.substring(upto));
            }
            ts.close();

            return fragments;
          }
        };
    } else {
      suggester = null;
      assert false;
    }

    state.suggesters.put(suggestName, suggester);

    return suggester;
  }

  /** Used to return highlighted result; see {@link
   *  LookupResult#highlightKey} */
  public static final class LookupHighlightFragment {
    /** Portion of text for this fragment. */
    public final String text;

    /** True if this text matched a part of the user's
     *  query. */
    public final boolean isHit;

    /** Sole constructor. */
    public LookupHighlightFragment(String text, boolean isHit) {
      this.text = text;
      this.isHit = isHit;
    }

    @Override
    public String toString() {
      return "LookupHighlightFragment(text=" + text + " isHit=" + isHit + ")";
    }
  }

  /** Wraps another {@link InputIterator} and counts how
   *  many suggestions were seen. */
  private static class CountingInputIterator implements InputIterator {

    private final InputIterator other;
    private int count;

    public CountingInputIterator(InputIterator other) {
      this.other = other;
    }

    @Override
    public long weight() {
      return other.weight();
    }

    @Override
    public BytesRef next() throws IOException {
      BytesRef result = other.next();
      if (result != null) {
        count++;
      }

      return result;
    }

    @Override
    public BytesRef payload() {
      return other.payload();
    }

    @Override
    public boolean hasPayloads() {
      return other.hasPayloads();
    }

    public int getCount() {
      return count;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    final String jsonOrig = r.toString();

    final String suggestName = r.getString("suggestName");
    if (!IndexState.isSimpleName(suggestName)) {
      r.fail("suggestName", "invalid suggestName \"" + suggestName + "\": must be [a-zA-Z_][a-zA-Z0-9]*");
    }

    final Lookup suggester = getSuggester(state, suggestName, r);

    Request source = r.getStruct("source");

    InputIterator iterator = null;
    final SearcherAndTaxonomy searcher;

    if (source.hasParam("localFile")) {
      final File localFile = new File(source.getString("localFile"));
      if (!localFile.exists()) {
        r.fail("localFile", "file does not exist");
      }
      if (!localFile.canRead()) {
        r.fail("localFile", "cannot read file");
      }
      searcher = null;
      // Pull suggestions from local file:
      try {
        iterator = new FromFileTermFreqIterator(localFile);
      } catch (IOException ioe) {
        r.fail("localFile", "cannot open file", ioe);
      }
    } else {

      state.verifyStarted(r);

      // Pull suggestions from stored docs:
      if (source.hasParam("searcher")) {
        // Specific searcher version:
        searcher = SearchHandler.getSearcherAndTaxonomy(source, state, null);
      } else {
        // Just use current searcher version:
        searcher = state.manager.acquire();
      }
      String suggestField = source.getString("suggestField");

      String payloadField;
      if (source.hasParam("payloadField")) {
        payloadField = source.getString("payloadField");
      } else {
        payloadField = null;
      }

      DocumentDictionary dict;

      if (source.hasParam("weightField")) {
        // Weight is a field
        String weightField = source.getString("weightField");
        dict = new DocumentDictionary(searcher.searcher.getIndexReader(),
                                      suggestField,
                                      weightField,
                                      payloadField);
      } else {
        // Weight is an expression; add bindings for all
        // numeric DV fields:
        Expression expr;
        try {
          expr = JavascriptCompiler.compile(source.getString("weightExpression"));
        } catch (Exception e) {
          source.fail("weightExpression", "expression does not compile", e);
          // Dead code but compiler disagrees
          expr = null;
        }
        
        dict = new DocumentValueSourceDictionary(searcher.searcher.getIndexReader(),
                                                 suggestField,
                                                 expr.getValueSource(state.exprBindings),
                                                 payloadField);
      }

      iterator = dict.getEntryIterator();
    }

    // nocommit return error if suggester already exists?

    // nocommit need a DeleteSuggestHandler

    final InputIterator iterator0 = iterator;
    
    return new FinishRequest() {

      @Override
      public String finish() throws IOException {

        try {
          suggester.build(iterator0);
        } finally {
          if (iterator0 instanceof Closeable) {
            ((Closeable) iterator0).close();
          }
          if (searcher != null) {
            state.manager.release(searcher);
          }
        }

        File outFile = new File(state.rootDir, "suggest." + suggestName);
        OutputStream out = new FileOutputStream(outFile);
        boolean success = false;
        try {
          // nocommit look @ return value
          suggester.store(out);
          success = true;
        } finally {
          out.close();
          if (!success) {
            outFile.delete();
          }
        }

        //System.out.println("buildSuggest: now add suggestName=" + suggestName + " to IndexState.name=" + state.name);

        try {
          state.addSuggest(suggestName, (JSONObject) JSONValue.parseStrict(jsonOrig));
        } catch (ParseException pe) {
          // BUG
          throw new RuntimeException(pe);
        }

        JSONObject ret = new JSONObject();
        if (suggester instanceof AnalyzingSuggester) {
          ret.put("sizeInBytes", ((AnalyzingSuggester) suggester).sizeInBytes());
        }

        ret.put("count", suggester.getCount());
        return ret.toString();
      }
    };
  }
}
