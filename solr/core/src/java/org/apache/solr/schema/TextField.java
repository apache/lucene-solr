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
package org.apache.solr.schema;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.QueryBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.parser.SolrQueryParserBase;
import org.apache.solr.query.SolrRangeQuery;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/** <code>TextField</code> is the basic type for configurable text analysis.
 * Analyzers for field types using this implementation should be defined in the schema.
 *
 */
public class TextField extends FieldType {
  protected boolean autoGeneratePhraseQueries;
  protected boolean enableGraphQueries;
  protected SolrQueryParserBase.SynonymQueryStyle synonymQueryStyle;

  /**
   * Analyzer set by schema for text types to use when searching fields
   * of this type, subclasses can set analyzer themselves or override
   * getIndexAnalyzer()
   * This analyzer is used to process wildcard, prefix, regex and other multiterm queries. It
   * assembles a list of tokenizer +filters that "make sense" for this, primarily accent folding and
   * lowercasing filters, and charfilters.
   *
   * @see #getMultiTermAnalyzer
   * @see #setMultiTermAnalyzer
   */
  protected Analyzer multiTermAnalyzer=null;
  private boolean isExplicitMultiTermAnalyzer = false;

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    properties |= TOKENIZED;
    if (schema.getVersion() > 1.1F &&
        // only override if it's not explicitly true
        0 == (trueProperties & OMIT_TF_POSITIONS)) {
      properties &= ~OMIT_TF_POSITIONS;
    }
    if (schema.getVersion() > 1.3F) {
      autoGeneratePhraseQueries = false;
    } else {
      autoGeneratePhraseQueries = true;
    }
    String autoGeneratePhraseQueriesStr = args.remove(AUTO_GENERATE_PHRASE_QUERIES);
    if (autoGeneratePhraseQueriesStr != null)
      autoGeneratePhraseQueries = Boolean.parseBoolean(autoGeneratePhraseQueriesStr);

    synonymQueryStyle = SolrQueryParserBase.SynonymQueryStyle.AS_SAME_TERM;
    String synonymQueryStyle = args.remove(SYNONYM_QUERY_STYLE);
    if (synonymQueryStyle != null) {
      this.synonymQueryStyle = SolrQueryParserBase.SynonymQueryStyle.valueOf(synonymQueryStyle.toUpperCase(Locale.ROOT));
    }
    
    enableGraphQueries = true;
    String enableGraphQueriesStr = args.remove(ENABLE_GRAPH_QUERIES);
    if (enableGraphQueriesStr != null)
      enableGraphQueries = Boolean.parseBoolean(enableGraphQueriesStr);

    super.init(schema, args);    
  }

  /**
   * Returns the Analyzer to be used when searching fields of this type when mult-term queries are specified.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getIndexAnalyzer
   */
  public Analyzer getMultiTermAnalyzer() {
    return multiTermAnalyzer;
  }

  public void setMultiTermAnalyzer(Analyzer analyzer) {
    this.multiTermAnalyzer = analyzer;
  }

  public boolean getAutoGeneratePhraseQueries() {
    return autoGeneratePhraseQueries;
  }
  
  public boolean getEnableGraphQueries() {
    return enableGraphQueries;
  }

  public SolrQueryParserBase.SynonymQueryStyle getSynonymQueryStyle() {return synonymQueryStyle;}

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    /* :TODO: maybe warn if isTokenized(), but doesn't use LimitTokenCountFilter in its chain? */
    return getSortedSetSortField(field,
                                 // historical behavior based on how the early versions of the FieldCache
                                 // would deal with multiple indexed terms in a singled valued field...
                                 //
                                 // Always use the 'min' value from the (Uninverted) "psuedo doc values"
                                 SortedSetSelector.Type.MIN,
                                 reverse, SortField.STRING_FIRST, SortField.STRING_LAST);
  }
  
  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return new SortedSetFieldSource(field.getName());
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    return Type.SORTED_SET_BINARY;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), true);
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return parseFieldQuery(parser, getQueryAnalyzer(), field.getName(), externalVal);
  }
  
  @Override
  public Query getFieldTermQuery(QParser parser, SchemaField field, String externalVal) {
    BytesRefBuilder br = new BytesRefBuilder();
    readableToIndexed(externalVal, br);
    return new TermQuery(new Term(field.getName(), br));
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.utf8ToString();
  }

  @Override
  protected boolean supportsAnalyzers() {
    return true;
  }

  @Override
  protected Query getSpecializedRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    Analyzer multiAnalyzer = getMultiTermAnalyzer();
    BytesRef lower = analyzeMultiTerm(field.getName(), part1, multiAnalyzer);
    BytesRef upper = analyzeMultiTerm(field.getName(), part2, multiAnalyzer);
    return new SolrRangeQuery(field.getName(), lower, upper, minInclusive, maxInclusive);
  }

  /**
   * Analyzes a text part using the provided {@link Analyzer} for a multi-term query.
   * <p>
   * Expects a single token to be used as multi-term term. This single token might also be filtered out
   * so zero token is supported and null is returned in this case.
   *
   * @return The multi-term term bytes; or null if there is no multi-term terms.
   * @throws SolrException If the {@link Analyzer} tokenizes more than one token;
   * or if an underlying {@link IOException} occurs.
   */
  public static BytesRef analyzeMultiTerm(String field, String part, Analyzer analyzerIn) {
    if (part == null || analyzerIn == null) return null;

    try (TokenStream source = analyzerIn.tokenStream(field, part)){
      source.reset();

      TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);

      if (!source.incrementToken()) {
        // Accept no tokens because it may have been filtered out by a StopFilter for example.
        return null;
      }
      BytesRef bytes = BytesRef.deepCopyOf(termAtt.getBytesRef());
      if (source.incrementToken())
        throw  new SolrException(SolrException.ErrorCode.BAD_REQUEST,"analyzer returned too many terms for multiTerm term: " + part);

      source.end();
      return bytes;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"error analyzing range part: " + part, e);
    }
  }


  static Query parseFieldQuery(QParser parser, Analyzer analyzer, String field, String queryText) {
    // note, this method always worked this way (but nothing calls it?) because it has no idea of quotes...
    return new QueryBuilder(analyzer).createPhraseQuery(field, queryText);
  }

  public void setIsExplicitMultiTermAnalyzer(boolean isExplicitMultiTermAnalyzer) {
    this.isExplicitMultiTermAnalyzer = isExplicitMultiTermAnalyzer;
  }

  public boolean isExplicitMultiTermAnalyzer() {
    return isExplicitMultiTermAnalyzer;
  }

  @Override
  public Object marshalSortValue(Object value) {
    return marshalStringSortValue(value);
  }

  @Override
  public Object unmarshalSortValue(Object value) {
    return unmarshalStringSortValue(value);
  }

  @Override
  public boolean isUtf8Field() {
    return true;
  }
}
