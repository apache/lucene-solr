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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

/** An {@link CompositeReader} which reads multiple, parallel indexes.  Each index added
 * must have the same number of documents, and exactly the same hierarchical subreader structure,
 * but typically each contains different fields. Deletions are taken from the first reader.
 * Each document contains the union of the fields of all
 * documents with the same document number.  When searching, matches for a
 * query term are from the first index added that has the field.
 *
 * <p>This is useful, e.g., with collections that have large fields which
 * change rarely and small fields that change more frequently.  The smaller
 * fields may be re-indexed in a new index and both indexes may be searched
 * together.
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
 */
public final class ParallelCompositeReader extends BaseCompositeReader<IndexReader> {
  private final boolean closeSubReaders;
  private final Set<CompositeReader> completeReaderSet =
    Collections.newSetFromMap(new IdentityHashMap<CompositeReader,Boolean>());

  /** Create a ParallelCompositeReader based on the provided
   *  readers; auto-closes the given readers on {@link #close()}. */
  public ParallelCompositeReader(CompositeReader... readers) throws IOException {
    this(true, readers);
  }

  /** Create a ParallelCompositeReader based on the provided
   *  readers. */
  public ParallelCompositeReader(boolean closeSubReaders, CompositeReader... readers) throws IOException {
    this(closeSubReaders, readers, readers);
  }

  /** Expert: create a ParallelCompositeReader based on the provided
   *  readers and storedFieldReaders; when a document is
   *  loaded, only storedFieldsReaders will be used. */
  public ParallelCompositeReader(boolean closeSubReaders, CompositeReader[] readers, CompositeReader[] storedFieldReaders) throws IOException {
    super(prepareSubReaders(readers, storedFieldReaders));
    this.closeSubReaders = closeSubReaders;
    Collections.addAll(completeReaderSet, readers);
    Collections.addAll(completeReaderSet, storedFieldReaders);
    // do this finally so any Exceptions occurred before don't affect refcounts:
    if (!closeSubReaders) {
      for (CompositeReader reader : completeReaderSet) {
        reader.incRef();
      }
    }
  }

  private static IndexReader[] prepareSubReaders(CompositeReader[] readers, CompositeReader[] storedFieldsReaders) throws IOException {
    if (readers.length == 0) {
      if (storedFieldsReaders.length > 0)
        throw new IllegalArgumentException("There must be at least one main reader if storedFieldsReaders are used.");
      return new IndexReader[0];
    } else {
      final IndexReader[] firstSubReaders = readers[0].getSequentialSubReaders();

      // check compatibility:
      final int maxDoc = readers[0].maxDoc();
      final int[] childMaxDoc = new int[firstSubReaders.length];
      final boolean[] childAtomic = new boolean[firstSubReaders.length];
      for (int i = 0; i < firstSubReaders.length; i++) {
        childMaxDoc[i] = firstSubReaders[i].maxDoc();
        childAtomic[i] = firstSubReaders[i] instanceof AtomicReader;
      }
      validate(readers, maxDoc, childMaxDoc, childAtomic);
      validate(storedFieldsReaders, maxDoc, childMaxDoc, childAtomic);

      // hierarchically build the same subreader structure as the first CompositeReader with Parallel*Readers:
      final IndexReader[] subReaders = new IndexReader[firstSubReaders.length];
      for (int i = 0; i < subReaders.length; i++) {
        if (firstSubReaders[i] instanceof AtomicReader) {
          final AtomicReader[] atomicSubs = new AtomicReader[readers.length];
          for (int j = 0; j < readers.length; j++) {
            atomicSubs[j] = (AtomicReader) readers[j].getSequentialSubReaders()[i];
          }
          final AtomicReader[] storedSubs = new AtomicReader[storedFieldsReaders.length];
          for (int j = 0; j < storedFieldsReaders.length; j++) {
            storedSubs[j] = (AtomicReader) storedFieldsReaders[j].getSequentialSubReaders()[i];
          }
          // we simply enable closing of subReaders, to prevent incRefs on subReaders
          // -> for synthetic subReaders, close() is never
          // called by our doClose()
          subReaders[i] = new ParallelAtomicReader(true, atomicSubs, storedSubs);
        } else {
          assert firstSubReaders[i] instanceof CompositeReader;
          final CompositeReader[] compositeSubs = new CompositeReader[readers.length];
          for (int j = 0; j < readers.length; j++) {
            compositeSubs[j] = (CompositeReader) readers[j].getSequentialSubReaders()[i];
          }
          final CompositeReader[] storedSubs = new CompositeReader[storedFieldsReaders.length];
          for (int j = 0; j < storedFieldsReaders.length; j++) {
            storedSubs[j] = (CompositeReader) storedFieldsReaders[j].getSequentialSubReaders()[i];
          }
          // we simply enable closing of subReaders, to prevent incRefs on subReaders
          // -> for synthetic subReaders, close() is never called by our doClose()
          subReaders[i] = new ParallelCompositeReader(true, compositeSubs, storedSubs);
        }
      }
      return subReaders;
    }
  }
  
  private static void validate(CompositeReader[] readers, int maxDoc, int[] childMaxDoc, boolean[] childAtomic) {
    for (int i = 0; i < readers.length; i++) {
      final CompositeReader reader = readers[i];
      final IndexReader[] subs = reader.getSequentialSubReaders();
      if (reader.maxDoc() != maxDoc) {
        throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
      }
      if (subs.length != childMaxDoc.length) {
        throw new IllegalArgumentException("All readers must have same number of subReaders");
      }
      for (int subIDX = 0; subIDX < subs.length; subIDX++) {
        if (subs[subIDX].maxDoc() != childMaxDoc[subIDX]) {
          throw new IllegalArgumentException("All readers must have same corresponding subReader maxDoc");
        }
        if (!(childAtomic[subIDX] ? (subs[subIDX] instanceof AtomicReader) : (subs[subIDX] instanceof CompositeReader))) {
          throw new IllegalArgumentException("All readers must have same corresponding subReader types (atomic or composite)");
        }
      }
    }    
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("ParallelCompositeReader(");
    for (final Iterator<CompositeReader> iter = completeReaderSet.iterator(); iter.hasNext();) {
      buffer.append(iter.next());
      if (iter.hasNext()) buffer.append(", ");
    }
    return buffer.append(')').toString();
  }
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (final CompositeReader reader : completeReaderSet) {
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
}
