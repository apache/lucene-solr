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

package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.memory.MemoryIndex;

class OffsetsReader extends FilterLeafReader {

  private final int doc;
  private final Map<String, Terms> fromTermVectors;
  private final Set<String> fromAnalysis;
  private final LeafReader reanalyzedReader;

  OffsetsReader(LeafReader reader, int doc, Map<String, Terms> fromTermVectors, Set<String> fromAnalysis, Analyzer analyzer) throws IOException {
    super(reader);
    this.doc = doc;
    this.fromTermVectors = fromTermVectors;
    this.fromAnalysis = fromAnalysis;
    MemoryIndex mi = new MemoryIndex(true);
    reader.document(doc, new StoredFieldVisitor() {
      @Override
      public Status needsField(FieldInfo fieldInfo) throws IOException {
        return fromAnalysis.contains(fieldInfo.name) ? Status.YES : Status.NO;
      }

      @Override
      public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
        mi.addField(fieldInfo.name, new String(value), analyzer);
      }
    });
    mi.freeze();
    this.reanalyzedReader = (LeafReader) mi.createSearcher().getIndexReader();
  }

  @Override
  public Terms terms(String field) throws IOException {
    if (fromTermVectors.containsKey(field)) {
      return new SingleDocTerms(fromTermVectors.get(field));
    }
    if (fromAnalysis.contains(field)) {
      return new SingleDocTerms(reanalyzedReader.terms(field));
    }
    return in.terms(field);
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }

  private class SingleDocTerms extends FilterTerms {

    SingleDocTerms(Terms in) {
      super(in);
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new FilterTermsEnum(in.iterator()) {
        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
          return new FilterPostingsEnum(in.postings(reuse, flags)) {
            @Override
            public int advance(int target) throws IOException {
              if (target == doc) {
                in.advance(0);
                return target;
              }
              return NO_MORE_DOCS;
            }
          };
        }
      };
    }
  }
}
