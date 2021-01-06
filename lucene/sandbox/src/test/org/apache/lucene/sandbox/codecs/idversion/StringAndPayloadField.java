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
package org.apache.lucene.sandbox.codecs.idversion;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

// TODO: can we take a BytesRef token instead?

/** Produces a single String token from the provided value, with the provided payload. */
class StringAndPayloadField extends Field {

  public static final FieldType TYPE = new FieldType();

  static {
    TYPE.setOmitNorms(true);
    TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    TYPE.setTokenized(true);
    TYPE.freeze();
  }

  private final BytesRef payload;

  public StringAndPayloadField(String name, String value, BytesRef payload) {
    super(name, value, TYPE);
    this.payload = payload;
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    SingleTokenWithPayloadTokenStream ts;
    if (reuse instanceof SingleTokenWithPayloadTokenStream) {
      ts = (SingleTokenWithPayloadTokenStream) reuse;
    } else {
      ts = new SingleTokenWithPayloadTokenStream();
    }
    ts.setValue((String) fieldsData, payload);
    return ts;
  }

  static final class SingleTokenWithPayloadTokenStream extends TokenStream {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute payloadAttribute = addAttribute(PayloadAttribute.class);
    private boolean used = false;
    private String value = null;
    private BytesRef payload;

    /** Sets the string value. */
    void setValue(String value, BytesRef payload) {
      this.value = value;
      this.payload = payload;
    }

    @Override
    public boolean incrementToken() {
      if (used) {
        return false;
      }
      clearAttributes();
      termAttribute.append(value);
      payloadAttribute.setPayload(payload);
      used = true;
      return true;
    }

    @Override
    public void reset() {
      used = false;
    }

    @Override
    public void close() {
      value = null;
      payload = null;
    }
  }
}
