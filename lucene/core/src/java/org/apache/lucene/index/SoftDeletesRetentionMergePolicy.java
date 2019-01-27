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

package org.apache.lucene.index;


import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;

/**
 * This {@link MergePolicy} allows to carry over soft deleted documents across merges. The policy wraps
 * the merge reader and marks documents as "live" that have a value in the soft delete field and match the
 * provided query. This allows for instance to keep documents alive based on time or any other constraint in the index.
 * The main purpose for this merge policy is to implement retention policies for document modification to vanish in the
 * index. Using this merge policy allows to control when soft deletes are claimed by merges.
 * @lucene.experimental
 */
public final class SoftDeletesRetentionMergePolicy extends OneMergeWrappingMergePolicy {
  private final String field;
  private final Supplier<Query> retentionQuerySupplier;
  /**
   * Creates a new {@link SoftDeletesRetentionMergePolicy}
   * @param field the soft deletes field
   * @param retentionQuerySupplier a query supplier for the retention query
   * @param in the wrapped MergePolicy
   */
  public SoftDeletesRetentionMergePolicy(String field, Supplier<Query> retentionQuerySupplier, MergePolicy in) {
    super(in, toWrap -> new MergePolicy.OneMerge(toWrap.segments) {
      @Override
      public CodecReader wrapForMerge(CodecReader reader) throws IOException {
        CodecReader wrapped = toWrap.wrapForMerge(reader);
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) { // no deletes - just keep going
          return wrapped;
        }
        return applyRetentionQuery(field, retentionQuerySupplier.get(), wrapped);
      }
    });
    Objects.requireNonNull(field, "field must not be null");
    Objects.requireNonNull(retentionQuerySupplier, "retentionQuerySupplier must not be null");
    this.field = field;
    this.retentionQuerySupplier = retentionQuerySupplier;
  }

  @Override
  public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    CodecReader reader = readerIOSupplier.get();
    /* we only need a single hit to keep it no need for soft deletes to be checked*/
    Scorer scorer = getScorer(retentionQuerySupplier.get(), FilterCodecReader.wrapLiveDocs(reader, null, reader.maxDoc()));
    if (scorer != null) {
      DocIdSetIterator iterator = scorer.iterator();
      boolean atLeastOneHit = iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
      return atLeastOneHit;
    }
    return super.keepFullyDeletedSegment(readerIOSupplier) ;
  }

  // pkg private for testing
  static CodecReader applyRetentionQuery(String softDeleteField, Query retentionQuery, CodecReader reader) throws IOException {
    Bits liveDocs = reader.getLiveDocs();
    if (liveDocs == null) { // no deletes - just keep going
      return reader;
    }
    CodecReader wrappedReader = FilterCodecReader.wrapLiveDocs(reader, new Bits() { // only search deleted
      @Override
      public boolean get(int index) {
        return liveDocs.get(index) == false;
      }

      @Override
      public int length() {
        return liveDocs.length();
      }
    }, reader.maxDoc() - reader.numDocs());
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new DocValuesFieldExistsQuery(softDeleteField), BooleanClause.Occur.FILTER);
    builder.add(retentionQuery, BooleanClause.Occur.FILTER);
    Scorer scorer = getScorer(builder.build(), wrappedReader);
    if (scorer != null) {
      FixedBitSet cloneLiveDocs = FixedBitSet.copyOf(liveDocs);
      DocIdSetIterator iterator = scorer.iterator();
      int numExtraLiveDocs = 0;
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        if (cloneLiveDocs.getAndSet(iterator.docID()) == false) {
          // if we bring one back to live we need to account for it
          numExtraLiveDocs++;
        }
      }
      assert reader.numDocs() + numExtraLiveDocs <= reader.maxDoc() : "numDocs: " + reader.numDocs() + " numExtraLiveDocs: " + numExtraLiveDocs + " maxDoc: " + reader.maxDoc();
      return FilterCodecReader.wrapLiveDocs(reader, cloneLiveDocs, reader.numDocs() + numExtraLiveDocs);
    } else {
      return reader;
    }
  }

  private static Scorer getScorer(Query query, CodecReader reader) throws IOException {
    IndexSearcher s = new IndexSearcher(reader);
    s.setQueryCache(null);
    Weight weight = s.createWeight(s.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    return weight.scorer(reader.getContext());
  }

  @Override
  public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) throws IOException {
    final int numDeletesToMerge = super.numDeletesToMerge(info, delCount, readerSupplier);
    if (numDeletesToMerge != 0 && info.getSoftDelCount() > 0) {
      final CodecReader reader = readerSupplier.get();
      if (reader.getLiveDocs() != null) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new DocValuesFieldExistsQuery(field), BooleanClause.Occur.FILTER);
        builder.add(retentionQuerySupplier.get(), BooleanClause.Occur.FILTER);
        Scorer scorer = getScorer(builder.build(), FilterCodecReader.wrapLiveDocs(reader, null, reader.maxDoc()));
        if (scorer != null) {
          DocIdSetIterator iterator = scorer.iterator();
          Bits liveDocs = reader.getLiveDocs();
          int numDeletedDocs = reader.numDeletedDocs();
          while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs.get(iterator.docID()) == false) {
              numDeletedDocs--;
            }
          }
          return numDeletedDocs;
        }
      }
    }
    assert numDeletesToMerge >= 0 : "numDeletesToMerge: " + numDeletesToMerge;
    assert numDeletesToMerge <= info.info.maxDoc() : "numDeletesToMerge: " + numDeletesToMerge + " maxDoc:" + info.info.maxDoc();
    return numDeletesToMerge;
  }
}
