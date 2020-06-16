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

package org.apache.solr.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.queries.payloads.PayloadDecoder;
import org.apache.lucene.queries.payloads.PayloadFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

public class FloatPayloadValueSource extends ValueSource {
  protected final String field;
  protected final String val;
  protected final String indexedField;
  protected final BytesRef indexedBytes;
  protected final PayloadDecoder decoder;
  protected final PayloadFunction payloadFunction;
  protected final ValueSource defaultValueSource;

  public FloatPayloadValueSource(String field, String val, String indexedField, BytesRef indexedBytes,
                                 PayloadDecoder decoder, PayloadFunction payloadFunction, ValueSource defaultValueSource) {
    this.field = field;
    this.val = val;
    this.indexedField = indexedField;
    this.indexedBytes = indexedBytes;
    this.decoder = decoder;
    this.payloadFunction = payloadFunction;
    this.defaultValueSource = defaultValueSource;
  }

  @Override
  public FunctionValues getValues(@SuppressWarnings({"rawtypes"})Map context
          , LeafReaderContext readerContext) throws IOException {

    final Terms terms = readerContext.reader().terms(indexedField);

    @SuppressWarnings({"unchecked"})
    FunctionValues defaultValues = defaultValueSource.getValues(context, readerContext);

    // copied the bulk of this from TFValueSource - TODO: this is a very repeated pattern - base-class this advance logic stuff?
    return new FloatDocValues(this) {
      PostingsEnum docs ;
      int atDoc;
      int lastDocRequested = -1;
      float docScore = 0.f;

      { reset(); }

      public void reset() throws IOException {
        // no one should call us for deleted docs?

        if (terms != null) {
          final TermsEnum termsEnum = terms.iterator();
          if (termsEnum.seekExact(indexedBytes)) {
            docs = termsEnum.postings(null, PostingsEnum.ALL);
          } else {
            docs = null;
          }
        } else {
          docs = null;
        }

        if (docs == null) {
          // dummy PostingsEnum so floatVal() can work
          // when would this be called?  if field/val did not match?  this is called for every doc?  create once and cache?
          docs = new PostingsEnum() {
            @Override
            public int freq() {
              return 0;
            }

            @Override
            public int nextPosition() throws IOException {
              return -1;
            }

            @Override
            public int startOffset() throws IOException {
              return -1;
            }

            @Override
            public int endOffset() throws IOException {
              return -1;
            }

            @Override
            public BytesRef getPayload() throws IOException {
              return null;
            }

            @Override
            public int docID() {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
              return 0;
            }
          };
        }
        atDoc = -1;
      }

      @Override
      public float floatVal(int doc) {
        try {
          if (doc < lastDocRequested) {
            // out-of-order access.... reset
            reset();
          }
          else if (doc == lastDocRequested) {
            return docScore;
          }
          
          lastDocRequested = doc;

          if (atDoc < doc) {
            atDoc = docs.advance(doc);
          }

          if (atDoc > doc) {
            // term doesn't match this document... either because we hit the
            // end, or because the next doc is after this doc.
            docScore =  defaultValues.floatVal(doc);
            return docScore;
          }

          // a match!
          int freq = docs.freq();
          int numPayloadsSeen = 0;
          float currentScore = 0;
          for (int i=0; i < freq; i++) {
            docs.nextPosition();
            BytesRef payload = docs.getPayload();
            if (payload != null) {
              float payloadVal = decoder.computePayloadFactor(payload);

              // payloadFunction = null represents "first"
              if (payloadFunction == null) return payloadVal;

              currentScore = payloadFunction.currentScore(doc, indexedField, docs.startOffset(), docs.endOffset(),
                  numPayloadsSeen, currentScore, payloadVal);
              numPayloadsSeen++;

            }
          }
          docScore =  (numPayloadsSeen > 0) ? payloadFunction.docScore(doc, indexedField, numPayloadsSeen, currentScore) : defaultValues.floatVal(doc);
          return docScore;
        } catch (IOException e) {
          throw new RuntimeException("caught exception in function "+description()+" : doc="+doc, e);
        }
      }
    };
  }

  // TODO: should this be formalized at the ValueSource level?  Seems to be the convention
  public String name() {
    return "payload";
  }

  @Override
  public String description() {
    return name() + '(' + field + ',' + val + ',' + defaultValueSource.toString() + ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FloatPayloadValueSource that = (FloatPayloadValueSource) o;

    if (!indexedField.equals(that.indexedField)) return false;
    if (indexedBytes != null ? !indexedBytes.equals(that.indexedBytes) : that.indexedBytes != null) return false;
    if (!decoder.equals(that.decoder)) return false;
    if (payloadFunction != null ? !payloadFunction.equals(that.payloadFunction) : that.payloadFunction != null)
      return false;
    return defaultValueSource.equals(that.defaultValueSource);

  }

  @Override
  public int hashCode() {
    int result = indexedField.hashCode();
    result = 31 * result + (indexedBytes != null ? indexedBytes.hashCode() : 0);
    result = 31 * result + decoder.hashCode();
    result = 31 * result + (payloadFunction != null ? payloadFunction.hashCode() : 0);
    result = 31 * result + defaultValueSource.hashCode();
    return result;
  }
}
