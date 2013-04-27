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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
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
public class ParallelCompositeReader extends BaseCompositeReader<IndexReader> {
  private final boolean closeSubReaders;
  private final Set<IndexReader> completeReaderSet =
    Collections.newSetFromMap(new IdentityHashMap<IndexReader,Boolean>());

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
    // update ref-counts (like MultiReader):
    if (!closeSubReaders) {
      for (final IndexReader reader : completeReaderSet) {
        reader.incRef();
      }
    }
    // finally add our own synthetic readers, so we close or decRef them, too (it does not matter what we do)
    completeReaderSet.addAll(getSequentialSubReaders());
  }

  private static IndexReader[] prepareSubReaders(CompositeReader[] readers, CompositeReader[] storedFieldsReaders) throws IOException {
    if (readers.length == 0) {
      if (storedFieldsReaders.length > 0)
        throw new IllegalArgumentException("There must be at least one main reader if storedFieldsReaders are used.");
      return new IndexReader[0];
    } else {
      final List<? extends IndexReader> firstSubReaders = readers[0].getSequentialSubReaders();

      // check compatibility:
      final int maxDoc = readers[0].maxDoc(), noSubs = firstSubReaders.size();
      final int[] childMaxDoc = new int[noSubs];
      final boolean[] childAtomic = new boolean[noSubs];
      for (int i = 0; i < noSubs; i++) {
        final IndexReader r = firstSubReaders.get(i);
        childMaxDoc[i] = r.maxDoc();
        childAtomic[i] = r instanceof AtomicReader;
      }
      validate(readers, maxDoc, childMaxDoc, childAtomic);
      validate(storedFieldsReaders, maxDoc, childMaxDoc, childAtomic);

      // hierarchically build the same subreader structure as the first CompositeReader with Parallel*Readers:
      final IndexReader[] subReaders = new IndexReader[noSubs];
      for (int i = 0; i < subReaders.length; i++) {
        if (firstSubReaders.get(i) instanceof AtomicReader) {
          final AtomicReader[] atomicSubs = new AtomicReader[readers.length];
          for (int j = 0; j < readers.length; j++) {
            atomicSubs[j] = (AtomicReader) readers[j].getSequentialSubReaders().get(i);
          }
          final AtomicReader[] storedSubs = new AtomicReader[storedFieldsReaders.length];
          for (int j = 0; j < storedFieldsReaders.length; j++) {
            storedSubs[j] = (AtomicReader) storedFieldsReaders[j].getSequentialSubReaders().get(i);
          }
          // We pass true for closeSubs and we prevent closing of subreaders in doClose():
          // By this the synthetic throw-away readers used here are completely invisible to ref-counting
          subReaders[i] = new ParallelAtomicReader(true, atomicSubs, storedSubs) {
            @Override
            protected void doClose() {}
          };
        } else {
          assert firstSubReaders.get(i) instanceof CompositeReader;
          final CompositeReader[] compositeSubs = new CompositeReader[readers.length];
          for (int j = 0; j < readers.length; j++) {
            compositeSubs[j] = (CompositeReader) readers[j].getSequentialSubReaders().get(i);
          }
          final CompositeReader[] storedSubs = new CompositeReader[storedFieldsReaders.length];
          for (int j = 0; j < storedFieldsReaders.length; j++) {
            storedSubs[j] = (CompositeReader) storedFieldsReaders[j].getSequentialSubReaders().get(i);
          }
          // We pass true for closeSubs and we prevent closing of subreaders in doClose():
          // By this the synthetic throw-away readers used here are completely invisible to ref-counting
          subReaders[i] = new ParallelCompositeReader(true, compositeSubs, storedSubs) {
            @Override
            protected void doClose() {}
          };
        }
      }
      return subReaders;
    }
  }
  
  private static void validate(CompositeReader[] readers, int maxDoc, int[] childMaxDoc, boolean[] childAtomic) {
    for (int i = 0; i < readers.length; i++) {
      final CompositeReader reader = readers[i];
      final List<? extends IndexReader> subs = reader.getSequentialSubReaders();
      if (reader.maxDoc() != maxDoc) {
        throw new IllegalArgumentException("All readers must have same maxDoc: "+maxDoc+"!="+reader.maxDoc());
      }
      final int noSubs = subs.size();
      if (noSubs != childMaxDoc.length) {
        throw new IllegalArgumentException("All readers must have same number of subReaders");
      }
      for (int subIDX = 0; subIDX < noSubs; subIDX++) {
        final IndexReader r = subs.get(subIDX);
        if (r.maxDoc() != childMaxDoc[subIDX]) {
          throw new IllegalArgumentException("All readers must have same corresponding subReader maxDoc");
        }
        if (!(childAtomic[subIDX] ? (r instanceof AtomicReader) : (r instanceof CompositeReader))) {
          throw new IllegalArgumentException("All readers must have same corresponding subReader types (atomic or composite)");
        }
      }
    }    
  }
  
  @Override
  protected synchronized void doClose() throws IOException {
    IOException ioe = null;
    for (final IndexReader reader : completeReaderSet) {
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
