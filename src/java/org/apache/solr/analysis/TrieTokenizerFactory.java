/**
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.DateField;
import static org.apache.solr.schema.TrieField.TrieTypes;

import java.io.IOException;
import java.io.Reader;

/**
 * Tokenizer for trie fields. It uses NumericTokenStream to create multiple trie encoded string per number.
 * Each string created by this tokenizer for a given number differs from the previous by the given precisionStep.
 * For query time token streams that only contain the highest precision term, use 32/64 as precisionStep.
 * <p/>
 * Refer to {@link org.apache.lucene.search.NumericRangeQuery} for more details.
 *
 * @version $Id$
 * @see org.apache.lucene.search.NumericRangeQuery
 * @see org.apache.solr.schema.TrieField
 * @since solr 1.4
 */
public class TrieTokenizerFactory extends BaseTokenizerFactory {
  protected static final DateField dateField = new DateField();
  protected final int precisionStep;
  protected final TrieTypes type;

  public TrieTokenizerFactory(TrieTypes type, int precisionStep) {
    this.type = type;
    this.precisionStep = precisionStep;
  }

  public TokenStream create(Reader input) {
    try {
      StringBuilder builder = new StringBuilder();
      char[] buf = new char[8];
      int len;
      while ((len = input.read(buf)) != -1)
        builder.append(buf, 0, len);
      switch (type) {
        case INTEGER:
          return new NumericTokenStream(precisionStep).setIntValue(Integer.parseInt(builder.toString()));
        case FLOAT:
          return new NumericTokenStream(precisionStep).setFloatValue(Float.parseFloat(builder.toString()));
        case LONG:
          return new NumericTokenStream(precisionStep).setLongValue(Long.parseLong(builder.toString()));
        case DOUBLE:
          return new NumericTokenStream(precisionStep).setDoubleValue(Double.parseDouble(builder.toString()));
        case DATE:
          return new NumericTokenStream(precisionStep).setLongValue(dateField.parseMath(null, builder.toString()).getTime());
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to create TrieIndexTokenizer", e);
    }
  }
}
