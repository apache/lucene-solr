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
package org.apache.solr.update;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.uninverting.UninvertingReader;

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
    return UninvertingReader.wrap(reader, schema.getUninversionMapper());
  }
  
  // we try to be well-behaved, but we are not (and IW's applyQueryDeletes isn't much better...)
  
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = in.rewrite(reader);
    if (rewritten != in) {
      return new DeleteByQueryWrapper(rewritten, schema);
    } else {
      return super.rewrite(reader);
    }
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final LeafReader wrapped = wrap((LeafReader) searcher.getIndexReader());
    final IndexSearcher privateContext = new IndexSearcher(wrapped);
    privateContext.setQueryCache(searcher.getQueryCache());
    final Weight inner = in.createWeight(privateContext, scoreMode, boost);
    return new Weight(DeleteByQueryWrapper.this) {
      @Override
      public void extractTerms(Set<Term> terms) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException { throw new UnsupportedOperationException(); }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return inner.scorer(privateContext.getIndexReader().leaves().get(0));
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return inner.isCacheable(ctx);
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
    int result = classHash();
    result = prime * result + Objects.hashCode(in);
    result = prime * result + Objects.hashCode(schema);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(DeleteByQueryWrapper other) {
    return Objects.equals(in, other.in) &&
           Objects.equals(schema, other.schema);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
