package org.apache.lucene.index;

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
import java.util.List;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * Split an index based on a {@link Filter}.
 */

public class PKIndexSplitter {
  private final Filter docsInFirstIndex;
  private final Directory input;
  private final Directory dir1;
  private final Directory dir2;
  private final IndexWriterConfig config1;
  private final IndexWriterConfig config2;
  
  /**
   * Split an index based on a {@link Filter}. All documents that match the filter
   * are sent to dir1, remaining ones to dir2.
   */
  public PKIndexSplitter(Version version, Directory input, Directory dir1, Directory dir2, Filter docsInFirstIndex) {
    this(input, dir1, dir2, docsInFirstIndex, newDefaultConfig(version), newDefaultConfig(version));
  }
  
  private static IndexWriterConfig newDefaultConfig(Version version) {
    return  new IndexWriterConfig(version, null).setOpenMode(OpenMode.CREATE);
  }
  
  public PKIndexSplitter(Directory input, Directory dir1, 
      Directory dir2, Filter docsInFirstIndex, IndexWriterConfig config1, IndexWriterConfig config2) {
    this.input = input;
    this.dir1 = dir1;
    this.dir2 = dir2;
    this.docsInFirstIndex = docsInFirstIndex;
    this.config1 = config1;
    this.config2 = config2;
  }
  
  /**
   * Split an index based on a  given primary key term 
   * and a 'middle' term.  If the middle term is present, it's
   * sent to dir2.
   */
  public PKIndexSplitter(Version version, Directory input, Directory dir1, Directory dir2, Term midTerm) {
    this(version, input, dir1, dir2,
      new TermRangeFilter(midTerm.field(), null, midTerm.bytes(), true, false));
  }
  
  public PKIndexSplitter(Directory input, Directory dir1, 
      Directory dir2, Term midTerm, IndexWriterConfig config1, IndexWriterConfig config2) {
    this(input, dir1, dir2,
      new TermRangeFilter(midTerm.field(), null, midTerm.bytes(), true, false), config1, config2);
  }
  
  public void split() throws IOException {
    boolean success = false;
    DirectoryReader reader = DirectoryReader.open(input);
    try {
      // pass an individual config in here since one config can not be reused!
      createIndex(config1, dir1, reader, docsInFirstIndex, false);
      createIndex(config2, dir2, reader, docsInFirstIndex, true);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader);
      } else {
        IOUtils.closeWhileHandlingException(reader);
      }
    }
  }
  
  private void createIndex(IndexWriterConfig config, Directory target, IndexReader reader, Filter preserveFilter, boolean negateFilter) throws IOException {
    boolean success = false;
    final IndexWriter w = new IndexWriter(target, config);
    try {
      final List<AtomicReaderContext> leaves = reader.leaves();
      final IndexReader[] subReaders = new IndexReader[leaves.size()];
      int i = 0;
      for (final AtomicReaderContext ctx : leaves) {
        subReaders[i++] = new DocumentFilteredAtomicIndexReader(ctx, preserveFilter, negateFilter);
      }
      w.addIndexes(subReaders);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(w);
      } else {
        IOUtils.closeWhileHandlingException(w);
      }
    }
  }
    
  private static class DocumentFilteredAtomicIndexReader extends FilterAtomicReader {
    final Bits liveDocs;
    final int numDocs;
    
    public DocumentFilteredAtomicIndexReader(AtomicReaderContext context, Filter preserveFilter, boolean negateFilter) throws IOException {
      super(context.reader());
      final int maxDoc = in.maxDoc();
      final FixedBitSet bits = new FixedBitSet(maxDoc);
      // ignore livedocs here, as we filter them later:
      final DocIdSet docs = preserveFilter.getDocIdSet(context, null);
      if (docs != null) {
        final DocIdSetIterator it = docs.iterator();
        if (it != null) {
          bits.or(it);
        }
      }
      if (negateFilter) {
        bits.flip(0, maxDoc);
      }

      if (in.hasDeletions()) {
        final Bits oldLiveDocs = in.getLiveDocs();
        assert oldLiveDocs != null;
        final DocIdSetIterator it = bits.iterator();
        for (int i = it.nextDoc(); i < maxDoc; i = it.nextDoc()) {
          if (!oldLiveDocs.get(i)) {
            // we can safely modify the current bit, as the iterator already stepped over it:
            bits.clear(i);
          }
        }
      }
      
      this.liveDocs = bits;
      this.numDocs = bits.cardinality();
    }
    
    @Override
    public int numDocs() {
      return numDocs;
    }
    
    @Override
    public Bits getLiveDocs() {
      return liveDocs;
    }
  }
}
