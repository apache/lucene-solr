package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

/** An {@link CompositeReader} which reads multiple, parallel indexes.  Each index added
 * must have the same number of documents, and exactly the same hierarchical subreader structure,
 * but typically each contains different fields. Each document contains the
 * union of the fields of all
 * documents with the same document number.  When searching, matches for a
 * query term are from the first index added that has the field.
 *
 * <p>This is useful, e.g., with collections that have large fields which
 * change rarely and small fields that change more frequently.  The smaller
 * fields may be re-indexed in a new index and both indexes may be searched
 * together.
 * 
 * <p>To create instances of {@code ParallelCompositeReader}, use the provided
 * {@link ParallelCompositeReader.Builder}.
 *
 * <p><strong>Warning:</strong> It is up to you to make sure all indexes
 * are created and modified the same way. For example, if you add
 * documents to one index, you need to add the same documents in the
 * same order to the other indexes. <em>Failure to do so will result in
 * undefined behavior</em>.
 * A good strategy to create suitable indexes with {@link IndexWriter} is to use
 * {@link LogDocMergePolicy}, as this one does not reorder documents
 * during merging (like {@code TieredMergePolicy}) and triggers merges
 * by number of documents per segment. If you use different {@link MergePolicy}s
 * it might happen that the segment structure of your index is no longer predictable.
 * {@link ParallelCompositeReader.Builder} will throw exceptions if the structure
 * of the underlying segments do not match for each parallel reader.
 */
public final class ParallelCompositeReader extends BaseMultiReader<IndexReader> {
  private final boolean closeSubReaders;
  private final CompositeReader parallelReaders[];
  
  // only called from builder!!!
  ParallelCompositeReader(boolean closeSubReaders, List<CompositeReader> parallelReaders, BitSet ignoreStoredFieldsSet) throws IOException {
    super(prepareSubReaders(parallelReaders, ignoreStoredFieldsSet));
    this.closeSubReaders = closeSubReaders;
    this.parallelReaders = parallelReaders.toArray(new CompositeReader[parallelReaders.size()]);
    if (!closeSubReaders) {
      for (CompositeReader reader : this.parallelReaders) {
        reader.incRef();
      }
    }
  }
  
  private static IndexReader[] prepareSubReaders(List<CompositeReader> parallelReaders, BitSet ignoreStoredFieldsSet) throws IOException {
    if (parallelReaders.isEmpty()) {
      return new IndexReader[0];
    } else {
      // hierarchically build the same subreader structure as the first CompositeReader with Parallel*Readers:
      final IndexReader[]
        firstSubReaders = parallelReaders.get(0).getSequentialSubReaders(),
        subReaders = new IndexReader[firstSubReaders.length];
      for (int i = 0; i < subReaders.length; i++) {
        if (firstSubReaders[i] instanceof AtomicReader) {
          // we simply enable closing of subReaders, to prevent incRefs on subReaders
          // -> for synthetic subReaders, close() is never called by our doClose()
          final ParallelAtomicReader.Builder builder = new ParallelAtomicReader.Builder(true); 
          for (int j = 0, c = parallelReaders.size(); j < c; j++) {
            builder.add((AtomicReader) parallelReaders.get(j).getSequentialSubReaders()[i], ignoreStoredFieldsSet.get(j));
          }
          subReaders[i] = builder.build();
        } else {
          assert firstSubReaders[i] instanceof CompositeReader;
          // we simply enable closing of subReaders, to prevent incRefs on subReaders
          // -> for synthetic subReaders, close() is never called by our doClose()
          final ParallelCompositeReader.Builder builder = new ParallelCompositeReader.Builder(true); 
          for (int j = 0, c = parallelReaders.size(); j < c; j++) {
            builder.add((CompositeReader) parallelReaders.get(j).getSequentialSubReaders()[i], ignoreStoredFieldsSet.get(j));
          }
          subReaders[i] = builder.build();
        }
      }
      return subReaders;
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelCompositeReader(");
    for (final Iterator<CompositeReader> iter = Arrays.asList(parallelReaders).iterator(); iter.hasNext();) {
      buffer.append(iter.next());
      if (iter.hasNext()) buffer.append(", ");
    }
    return buffer.append(')').toString();
  }
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (final CompositeReader reader : parallelReaders) {
      try {
        if (closeSubReaders) {
          reader.close();
        } else {
          reader.decRef();
        }
      } catch (IOException e) {
        if (ioe == null) ioe = e;
      }
    }
    // throw the first exception
    if (ioe != null) throw ioe;
  }
  
  /**
   * Builder implementation to create instances of {@link ParallelCompositeReader}.
   */
  public static final class Builder {
    private final boolean closeSubReaders;
    private final List<CompositeReader> readers = new ArrayList<CompositeReader>();
    private final BitSet ignoreStoredFieldsSet = new BitSet();
    private int[] leaveSizes, childSizes;
    private int maxDoc, numDocs;
    
    /**
     * Create a new builder instance that automatically enables closing of all subreader
     * once the build reader is closed.
     */
    public Builder() {
      this(true);
    }
    
    /**
     * Create a new builder instance.
     */
    public Builder(boolean closeSubReaders) {
      this.closeSubReaders = closeSubReaders;
    }
    
    /** Add an CompositeReader.
     * @throws IOException if there is a low-level IO error
     */
    public Builder add(CompositeReader reader) throws IOException {
      return add(reader, false);
    }
    
    /** Add an CompositeReader whose stored fields will not be returned.  This can
     * accelerate search when stored fields are only needed from a subset of
     * the IndexReaders.
     *
     * @throws IllegalArgumentException if not all indexes contain the same number
     *     of documents
     * @throws IllegalArgumentException if not all indexes have the same value
     *     of {@link AtomicReader#maxDoc()}
     * @throws IOException if there is a low-level IO error
     */
    public Builder add(CompositeReader reader, boolean ignoreStoredFields) throws IOException {
      final IndexReader[] subs = reader.getSequentialSubReaders();
      if (readers.isEmpty()) {
        this.maxDoc = reader.maxDoc();
        this.numDocs = reader.numDocs();
        childSizes = new int[subs.length];
        for (int i = 0; i < subs.length; i++) {
          childSizes[i] = subs[i].maxDoc();
        }
        final AtomicReaderContext[] leaves = reader.getTopReaderContext().leaves();
        leaveSizes = new int[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
          leaveSizes[i] = leaves[i].reader().maxDoc();
        }
      } else {
        // check compatibility
        if (reader.maxDoc() != maxDoc)
          throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
        if (reader.numDocs() != numDocs)
          throw new IllegalArgumentException("All readers must have same numDocs: "+numDocs+"!="+reader.numDocs());
        if (subs.length != childSizes.length)
          throw new IllegalArgumentException("All readers must have same number of subReaders");
        for (int i = 0; i < subs.length; i++) {
          if (subs[i].maxDoc() != childSizes[i])
            throw new IllegalArgumentException("All readers must have same subReader maxDoc");
        }
        // the following checks are only to detect errors early, otherwise a wrong leaf
        // structure would only cause errors on build(). These checks are still incomplete...
        final AtomicReaderContext[] leaves = reader.getTopReaderContext().leaves();
        if (leaves.length != leaveSizes.length)
          throw new IllegalArgumentException("All readers must have same number of atomic leaves");
        for (int i = 0; i < leaves.length; i++) {
          if (leaves[i].reader().maxDoc() != leaveSizes[i])
            throw new IllegalArgumentException("All readers must have atomic leaves with same maxDoc");
        }
      }
      
      ignoreStoredFieldsSet.set(readers.size(), ignoreStoredFields);
      readers.add(reader);
      return this;
    }
    
    /**
     * Build the {@link ParallelCompositeReader} instance from the settings.
     */
    public ParallelCompositeReader build() throws IOException {
      return new ParallelCompositeReader(closeSubReaders, readers, ignoreStoredFieldsSet);
    }
    
  }
}
