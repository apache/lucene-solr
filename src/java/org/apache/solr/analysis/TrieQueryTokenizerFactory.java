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
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.TrieField;

import java.io.IOException;
import java.io.Reader;

/**
 * Query time tokenizer for trie fields. It uses methods in TrieUtils to create a prefix coded representation of the
 * given number which is used for term queries.
 * <p/>
 * Note that queries on trie date types are not tokenized and returned as is.
 *
 * @version $Id$
 * @see org.apache.lucene.search.trie.TrieUtils
 * @see org.apache.solr.schema.TrieField
 * @since solr 1.4
 */
public class TrieQueryTokenizerFactory extends BaseTokenizerFactory {
  private final TrieField.TrieTypes type;

  public TrieQueryTokenizerFactory(TrieField.TrieTypes type) {
    this.type = type;
  }

  public TokenStream create(Reader reader) {
    try {
      return new TrieQueryTokenizer(reader, type);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to create TrieQueryTokenizer", e);
    }
  }

  public static class TrieQueryTokenizer extends Tokenizer {
    private static final DateField dateField = new DateField();

    private final String number;
    private boolean used = false;
    private TrieField.TrieTypes type;

    public TrieQueryTokenizer(Reader reader, TrieField.TrieTypes type) throws IOException {
      super(reader);
      this.type = type;
      StringBuilder builder = new StringBuilder();
      char[] buf = new char[8];
      int len;
      while ((len = reader.read(buf)) != -1)
        builder.append(buf, 0, len);
      number = builder.toString();
    }

    @Override
    public Token next(Token token) throws IOException {
      if (used) return null;
      String value = number;
      switch (type) {
        case INTEGER:
          value = TrieUtils.intToPrefixCoded(Integer.parseInt(number));
          break;
        case FLOAT:
          value = TrieUtils.intToPrefixCoded(TrieUtils.floatToSortableInt(Float.parseFloat(number)));
          break;
        case LONG:
          value = TrieUtils.longToPrefixCoded(Long.parseLong(number));
          break;
        case DOUBLE:
          value = TrieUtils.longToPrefixCoded(TrieUtils.doubleToSortableLong(Double.parseDouble(number)));
          break;
        case DATE:
          value = TrieUtils.longToPrefixCoded(dateField.parseMath(null, number).getTime());
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
      }
      token.reinit(value, 0, 0);
      token.setPositionIncrement(0);
      used = true;
      return token;
    }
  }
}
