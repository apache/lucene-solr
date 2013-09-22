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

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource.AttributeFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.DateField;
import static org.apache.solr.schema.TrieField.TrieTypes;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Tokenizer for trie fields. It uses NumericTokenStream to create multiple trie encoded string per number.
 * Each string created by this tokenizer for a given number differs from the previous by the given precisionStep.
 * For query time token streams that only contain the highest precision term, use 32/64 as precisionStep.
 * <p/>
 * Refer to {@link org.apache.lucene.search.NumericRangeQuery} for more details.
 *
 *
 * @see org.apache.lucene.search.NumericRangeQuery
 * @see org.apache.solr.schema.TrieField
 * @since solr 1.4
 */
public class TrieTokenizerFactory extends TokenizerFactory {
  protected final int precisionStep;
  protected final TrieTypes type;

  public TrieTokenizerFactory(TrieTypes type, int precisionStep) {
    super(new HashMap<String,String>());
    this.type = type;
    this.precisionStep = precisionStep;
  }

  @Override
  public TrieTokenizer create(AttributeFactory factory, Reader input) {
    return new TrieTokenizer(input, type, TrieTokenizer.getNumericTokenStream(factory, precisionStep));
  }
}

final class TrieTokenizer extends Tokenizer {
  protected static final DateField dateField = new DateField();
  protected final TrieTypes type;
  protected final NumericTokenStream ts;
  
  // NumericTokenStream does not support CharTermAttribute so keep it local
  private final CharTermAttribute termAtt = new CharTermAttributeImpl();
  protected final OffsetAttribute ofsAtt = addAttribute(OffsetAttribute.class);
  protected int startOfs, endOfs;
  protected boolean hasValue;

  static NumericTokenStream getNumericTokenStream(AttributeFactory factory, int precisionStep) {
    return new NumericTokenStream(factory, precisionStep);
  }

  public TrieTokenizer(Reader input, TrieTypes type, final NumericTokenStream ts) {
    // Häckidy-Hick-Hack: must share the attributes with the NumericTokenStream we delegate to, so we create a fake factory:
    super(new AttributeFactory() {
      @Override
      public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
        return (AttributeImpl) ts.addAttribute(attClass);
      }
    }, input);
    // add all attributes:
    for (Iterator<Class<? extends Attribute>> it = ts.getAttributeClassesIterator(); it.hasNext();) {
      addAttribute(it.next());
    }
    this.type = type;
    this.ts = ts;
    // dates tend to be longer, especially when math is involved
    termAtt.resizeBuffer( type == TrieTypes.DATE ? 128 : 32 );
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    try {
      int upto = 0;
      char[] buf = termAtt.buffer();
      while (true) {
        final int length = input.read(buf, upto, buf.length-upto);
        if (length == -1) break;
        upto += length;
        if (upto == buf.length)
          buf = termAtt.resizeBuffer(1+buf.length);
      }
      termAtt.setLength(upto);
      this.startOfs = correctOffset(0);
      this.endOfs = correctOffset(upto);
      
      if (upto == 0) {
        hasValue = false;
        return;
      }

      final String v = new String(buf, 0, upto);
      try {
        switch (type) {
          case INTEGER:
            ts.setIntValue(Integer.parseInt(v));
            break;
          case FLOAT:
            ts.setFloatValue(Float.parseFloat(v));
            break;
          case LONG:
            ts.setLongValue(Long.parseLong(v));
            break;
          case DOUBLE:
            ts.setDoubleValue(Double.parseDouble(v));
            break;
          case DATE:
            ts.setLongValue(dateField.parseMath(null, v).getTime());
            break;
          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
        }
      } catch (NumberFormatException nfe) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                                "Invalid Number: " + v);
      }
      hasValue = true;
      ts.reset();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to create TrieIndexTokenizer", e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (hasValue) {
      ts.close();
    }
  }

  @Override
  public boolean incrementToken() {
    if (hasValue && ts.incrementToken()) {
      ofsAtt.setOffset(startOfs, endOfs);
      return true;
    }
    return false;
  }

  @Override
  public void end() throws IOException {
    super.end();
    if (hasValue) {
      ts.end();
    }
    ofsAtt.setOffset(endOfs, endOfs);
  }
}
