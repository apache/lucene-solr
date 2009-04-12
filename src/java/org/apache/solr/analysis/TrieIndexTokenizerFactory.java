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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.search.trie.TrieUtils;
import org.apache.lucene.search.trie.IntTrieTokenStream;
import org.apache.lucene.search.trie.LongTrieTokenStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.DateField;
import static org.apache.solr.schema.TrieField.TrieTypes;

import java.io.IOException;
import java.io.Reader;

/**
 * Index time tokenizer for trie fields. It uses methods in TrieUtils to create multiple trie encoded string per number.
 * Each string created by this tokenizer for a given number differs from the previous by the given precisionStep.
 * <p/>
 * Refer to {@linkplain org.apache.lucene.search.trie package description} for more details.
 *
 * @version $Id$
 * @see org.apache.lucene.search.trie.TrieUtils
 * @see org.apache.solr.schema.TrieField
 * @since solr 1.4
 */
public class TrieIndexTokenizerFactory extends BaseTokenizerFactory {
  protected static final DateField dateField = new DateField();
  protected final int precisionStep;
  protected final TrieTypes type;

  public TrieIndexTokenizerFactory(TrieTypes type, int precisionStep) {
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
          return new IntTrieTokenStream(Integer.parseInt(builder.toString()), precisionStep);
        case FLOAT:
          return new IntTrieTokenStream(TrieUtils.floatToSortableInt(Float.parseFloat(builder.toString())), precisionStep);
        case LONG:
          return new LongTrieTokenStream(Long.parseLong(builder.toString()), precisionStep);
        case DOUBLE:
          return new LongTrieTokenStream(TrieUtils.doubleToSortableLong(Double.parseDouble(builder.toString())), precisionStep);
        case DATE:
          return new LongTrieTokenStream(dateField.parseMath(null, builder.toString()).getTime(), precisionStep);
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to create TrieIndexTokenizer", e);
    }
  }
}
