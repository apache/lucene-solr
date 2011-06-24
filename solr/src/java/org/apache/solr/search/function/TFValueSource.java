package org.apache.solr.search.function;

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

import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.util.Map;

public class TFValueSource extends TermFreqValueSource {
  public TFValueSource(String field, String val, String indexedField, BytesRef indexedBytes) {
    super(field, val, indexedField, indexedBytes);
  }

  @Override
  public String name() {
    return "tf";
  }

  @Override
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    Fields fields = readerContext.reader.fields();
    final Terms terms = fields.terms(field);
    final Similarity similarity = ((IndexSearcher)context.get("searcher")).getSimilarityProvider().get(field);

    return new FloatDocValues(this) {
      DocsEnum docs ;
      int atDoc;
      int lastDocRequested = -1;

      { reset(); }

      public void reset() throws IOException {
        // no one should call us for deleted docs?
        docs = terms==null ? null : terms.docs(null, indexedBytes, null);
        if (docs == null) {
          docs = new DocsEnum() {
            @Override
            public int freq() {
              return 0;
            }

            @Override
            public int docID() {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() throws IOException {
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
              return DocIdSetIterator.NO_MORE_DOCS;
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
          lastDocRequested = doc;

          if (atDoc < doc) {
            atDoc = docs.advance(doc);
          }

          if (atDoc > doc) {
            // term doesn't match this document... either because we hit the
            // end, or because the next doc is after this doc.
            return similarity.tf(0);
          }

          // a match!
          return similarity.tf(docs.freq());
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "caught exception in function "+description()+" : doc="+doc, e);
        }
      }
    };
  }
}
