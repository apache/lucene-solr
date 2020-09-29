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

package org.apache.solr.analysis;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a filter that will analyze tokens with the analyzer from an arbitrary field type.
 *
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_taf" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="com.example.NiftyTypeTwiddlerFilter"/&gt;
 *     &lt;filter class="solr.TokenAnalyzerFilter" asType="text_en" preserveType="true"/&gt;
 *     &lt;filter class="solr.TypeAsSynonymFilterFactory" prefix="__TAS__"
 *               ignore="word,&amp;lt;ALPHANUM&amp;gt;,&amp;lt;NUM&amp;gt;,&amp;lt;SOUTHEAST_ASIAN&amp;gt;,&amp;lt;IDEOGRAPHIC&amp;gt;,&amp;lt;HIRAGANA&amp;gt;,&amp;lt;KATAKANA&amp;gt;,&amp;lt;HANGUL&amp;gt;,&amp;lt;EMOJI&amp;gt;"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * Note that a configuration such as above may interfere with multi-word synonyms
 *
 *  @since 8.4
 *  @lucene.spi {@value #NAME}
 */
public class TokenAnalyzerFilterFactory extends TokenFilterFactory implements SchemaAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /** SPI name */
  public static final String NAME = "tokenAnalyzer";

  private IndexSchema schema;
  private String asType;
  private boolean preserveType;

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  public TokenAnalyzerFilterFactory(Map<String, String> args) {
    super(args);
    asType = super.require(args, "asType");
    preserveType = super.getBoolean(args,"preserveType",false);
  }

  private synchronized void lazyInit() {
    if (schema != null) {
      return;
    }
    // todo: it would be *awfully* nice if there were a better way to get
    //  added to the schema aware list.

    // holy hackery Batman! ... I know Robin, but we have no choice.
    // We need to be aware of the Toker's movements!
    synchronized (this) {
      // synch to avoid missing an inform occurring between these two statements.
      schema = SolrRequestInfo.getRequestInfo().getReq().getCore().getLatestSchema();
      schema.registerAware(this);
    }
  }

  private Analyzer acquireAnalyzer(IndexSchema latestSchema) {
    FieldType fieldType = latestSchema.getFieldTypeByName(asType);
    if (fieldType == null) {
      throw new RuntimeException("Could not find field type " + asType + " check that field type is defined and " +
          "your spelling in 'asType' matches the field's 'name' property");
    }
    // meh, but I see no better way... suggestions welcome
    boolean forQuery = SolrRequestInfo.getRequestInfo().getReq().getParams().getParams("q") != null;
    if (forQuery) {
      return fieldType.getQueryAnalyzer();
    } else {
      return fieldType.getIndexAnalyzer();
    }
  }

  @Override
  public synchronized void inform(IndexSchema schema) {
    this.schema = schema;
  }

  @Override
  public TokenStream create(TokenStream input) {
    lazyInit();
    return new TokenAnalyzerFilter(input, new AnalyzerProvider() {
    }, preserveType);
  }

  @Override
  public TokenStream normalize(TokenStream input) {
    // this might not be ideal, possibly we need to somehow build off of analyzer.normalize(), but that wants
    // entirely different arguments so the path forward is unclear.
    return create(input);
  }

  class AnalyzerProvider {
    Analyzer get() {
      return acquireAnalyzer(schema);
    }
  }

}


