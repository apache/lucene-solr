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
import java.util.Map;

import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.pattern.PatternReplaceFilterFactory;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;

/**
 * To be used for field {@link IndexSchema#NEST_PATH_FIELD_NAME} for enhanced
 * nested doc information.  By defining a field type, we can encapsulate the configuration
 * here so that the schema is free of it.  Alternatively, some notion of "implicit field types"
 * would be cool and a more general way of accomplishing this.
 *
 * @see org.apache.solr.update.processor.NestedUpdateProcessorFactory
 * @since 8.0
 */
public class NestPathField extends SortableTextField {

  @Override
  public void setArgs(IndexSchema schema, Map<String, String> args) {
    args.putIfAbsent("stored", "false");
    args.putIfAbsent("multiValued", "false");
    args.putIfAbsent("omitTermFreqAndPositions", "true");
    args.putIfAbsent("omitNorms", "true");
    args.putIfAbsent("maxCharsForDocValues", "-1");
    super.setArgs(schema, args);

    // CustomAnalyzer is easy to use
    CustomAnalyzer customAnalyzer;
    try {
      customAnalyzer = CustomAnalyzer.builder(schema.getResourceLoader())
          .withDefaultMatchVersion(schema.getDefaultLuceneMatchVersion())
          .withTokenizer(KeywordTokenizerFactory.class)
          .addTokenFilter(PatternReplaceFilterFactory.class,
              "pattern", "#\\d*",
              "replace", "all")
          .build();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);//impossible?
    }
    // Solr HTTP Schema APIs don't know about CustomAnalyzer so use TokenizerChain instead
    setIndexAnalyzer(new TokenizerChain(customAnalyzer));
    // leave queryAnalyzer as literal
  }

}
