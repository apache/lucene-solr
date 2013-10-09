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

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.QueryBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;

import java.util.Map;
import java.io.IOException;

/** <code>TextField</code> is the basic type for configurable text analysis.
 * Analyzers for field types using this implementation should be defined in the schema.
 *
 */
public class TextField extends FieldType {
  protected boolean autoGeneratePhraseQueries;

  /**
   * Analyzer set by schema for text types to use when searching fields
   * of this type, subclasses can set analyzer themselves or override
   * getAnalyzer()
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
    String autoGeneratePhraseQueriesStr = args.remove("autoGeneratePhraseQueries");
    if (autoGeneratePhraseQueriesStr != null)
      autoGeneratePhraseQueries = Boolean.parseBoolean(autoGeneratePhraseQueriesStr);
    super.init(schema, args);    
  }

  /**
   * Returns the Analyzer to be used when searching fields of this type when mult-term queries are specified.
   * <p>
   * This method may be called many times, at any time.
   * </p>
   * @see #getAnalyzer
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

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    /* :TODO: maybe warn if isTokenized(), but doesn't use LimitTokenCountFilter in it's chain? */
    return getStringSort(field, reverse);
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    return parseFieldQuery(parser, getQueryAnalyzer(), field.getName(), externalVal);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return term.utf8ToString();
  }

  @Override
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public void setQueryAnalyzer(Analyzer analyzer) {
    this.queryAnalyzer = analyzer;
  }


  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    Analyzer multiAnalyzer = getMultiTermAnalyzer();
    BytesRef lower = analyzeMultiTerm(field.getName(), part1, multiAnalyzer);
    BytesRef upper = analyzeMultiTerm(field.getName(), part2, multiAnalyzer);
    return new TermRangeQuery(field.getName(), lower, upper, minInclusive, maxInclusive);
  }

  public static BytesRef analyzeMultiTerm(String field, String part, Analyzer analyzerIn) {
    if (part == null || analyzerIn == null) return null;

    TokenStream source = null;
    try {
      source = analyzerIn.tokenStream(field, part);
      source.reset();

      TermToBytesRefAttribute termAtt = source.getAttribute(TermToBytesRefAttribute.class);
      BytesRef bytes = termAtt.getBytesRef();

      if (!source.incrementToken())
        throw  new SolrException(SolrException.ErrorCode.BAD_REQUEST,"analyzer returned no terms for multiTerm term: " + part);
      termAtt.fillBytesRef();
      if (source.incrementToken())
        throw  new SolrException(SolrException.ErrorCode.BAD_REQUEST,"analyzer returned too many terms for multiTerm term: " + part);

      source.end();
      return BytesRef.deepCopyOf(bytes);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"error analyzing range part: " + part, e);
    } finally {
      IOUtils.closeWhileHandlingException(source);
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
}
