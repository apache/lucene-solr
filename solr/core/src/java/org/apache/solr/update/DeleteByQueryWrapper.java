package org.apache.solr.update;

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

import java.io.IOException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.Bits;
import org.apache.solr.schema.IndexSchema;

/** 
 * Allows access to uninverted docvalues by delete-by-queries.
 * this is used e.g. to implement versioning constraints in solr.
 * <p>
 * Even though we wrap for each query, UninvertingReader's core 
 * cache key is the inner one, so it still reuses fieldcaches and so on.
 */
final class DeleteByQueryWrapper extends Query {
  final Query in;
  final IndexSchema schema;
  
  DeleteByQueryWrapper(Query in, IndexSchema schema) {
    this.in = in;
    this.schema = schema;
  }
  
  LeafReader wrap(LeafReader reader) {
    return new UninvertingReader(reader, schema.getUninversionMap(reader));
  }
  
  // we try to be well-behaved, but we are not (and IW's applyQueryDeletes isn't much better...)
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = in.rewrite(reader);
    if (rewritten != in) {
      return new DeleteByQueryWrapper(rewritten, schema);
    } else {
      return this;
    }
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    final LeafReader wrapped = wrap((LeafReader) searcher.getIndexReader());
    final IndexSearcher privateContext = new IndexSearcher(wrapped);
    final Weight inner = in.createWeight(privateContext);
    return new Weight() {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException { throw new UnsupportedOperationException(); }

      @Override
      public Query getQuery() { return DeleteByQueryWrapper.this; }

      @Override
      public float getValueForNormalization() throws IOException { return inner.getValueForNormalization(); }

      @Override
      public void normalize(float norm, float topLevelBoost) { inner.normalize(norm, topLevelBoost); }

      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        return inner.scorer(privateContext.getIndexReader().leaves().get(0), acceptDocs);
      }
    };
  }

  @Override
  public String toString(String field) {
    return "Uninverting(" + in.toString(field) + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((in == null) ? 0 : in.hashCode());
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    DeleteByQueryWrapper other = (DeleteByQueryWrapper) obj;
    if (in == null) {
      if (other.in != null) return false;
    } else if (!in.equals(other.in)) return false;
    if (schema == null) {
      if (other.schema != null) return false;
    } else if (!schema.equals(other.schema)) return false;
    return true;
  }
}
