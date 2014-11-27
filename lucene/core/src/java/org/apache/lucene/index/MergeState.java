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
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Holds common state used during segment merging.
 *
 * @lucene.experimental */
public class MergeState {

  /** {@link SegmentInfo} of the newly merged segment. */
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} of the newly merged segment. */
  public FieldInfos mergeFieldInfos;

  /** Stored field producers being merged */
  public final StoredFieldsReader[] storedFieldsReaders;

  /** Term vector producers being merged */
  public final TermVectorsReader[] termVectorsReaders;

  /** Norms producers being merged */
  public final NormsProducer[] normsProducers;

  /** DocValues producers being merged */
  public final DocValuesProducer[] docValuesProducers;

  /** FieldInfos being merged */
  public final FieldInfos[] fieldInfos;

  /** Live docs for each reader */
  public final Bits[] liveDocs;

  /** Maps docIDs around deletions. */
  public final DocMap[] docMaps;

  /** Postings to merge */
  public final FieldsProducer[] fieldsProducers;

  /** New docID base per reader. */
  public final int[] docBase;

  /** Max docs per reader */
  public final int[] maxDocs;

  /** Holds the CheckAbort instance, which is invoked
   *  periodically to see if the merge has been aborted. */
  public final CheckAbort checkAbort;

  /** InfoStream for debugging messages. */
  public final InfoStream infoStream;

  /** Counter used for periodic calls to checkAbort
   * @lucene.internal */
  public int checkAbortCount;

  /** Sole constructor. */
  MergeState(List<LeafReader> readers, SegmentInfo segmentInfo, InfoStream infoStream, CheckAbort checkAbort) throws IOException {

    int numReaders = readers.size();
    docMaps = new DocMap[numReaders];
    docBase = new int[numReaders];
    maxDocs = new int[numReaders];
    fieldsProducers = new FieldsProducer[numReaders];
    normsProducers = new NormsProducer[numReaders];
    storedFieldsReaders = new StoredFieldsReader[numReaders];
    termVectorsReaders = new TermVectorsReader[numReaders];
    docValuesProducers = new DocValuesProducer[numReaders];
    fieldInfos = new FieldInfos[numReaders];
    liveDocs = new Bits[numReaders];

    for(int i=0;i<numReaders;i++) {
      final LeafReader reader = readers.get(i);

      maxDocs[i] = reader.maxDoc();
      liveDocs[i] = reader.getLiveDocs();
      fieldInfos[i] = reader.getFieldInfos();

      NormsProducer normsProducer;
      DocValuesProducer docValuesProducer;
      StoredFieldsReader storedFieldsReader;
      TermVectorsReader termVectorsReader;
      FieldsProducer fieldsProducer;
      if (reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader;
        normsProducer = segmentReader.getNormsReader();
        if (normsProducer != null) {
          normsProducer = normsProducer.getMergeInstance();
        }
        docValuesProducer = segmentReader.getDocValuesReader();
        if (docValuesProducer != null) {
          docValuesProducer = docValuesProducer.getMergeInstance();
        }
        storedFieldsReader = segmentReader.getFieldsReader();
        if (storedFieldsReader != null) {
          storedFieldsReader = storedFieldsReader.getMergeInstance();
        }
        termVectorsReader = segmentReader.getTermVectorsReader();
        if (termVectorsReader != null) {
          termVectorsReader = termVectorsReader.getMergeInstance();
        }
        fieldsProducer = segmentReader.fields().getMergeInstance();
      } else {
        // A "foreign" reader
        normsProducer = readerToNormsProducer(reader);
        docValuesProducer = readerToDocValuesProducer(reader);
        storedFieldsReader = readerToStoredFieldsReader(reader);
        termVectorsReader = readerToTermVectorsReader(reader);
        fieldsProducer = readerToFieldsProducer(reader);
      }

      normsProducers[i] = normsProducer;
      docValuesProducers[i] = docValuesProducer;
      storedFieldsReaders[i] = storedFieldsReader;
      termVectorsReaders[i] = termVectorsReader;
      fieldsProducers[i] = fieldsProducer;
    }

    this.segmentInfo = segmentInfo;
    this.infoStream = infoStream;
    this.checkAbort = checkAbort;

    setDocMaps(readers);
  }

  private NormsProducer readerToNormsProducer(final LeafReader reader) {
    return new NormsProducer() {

      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        return reader.getNormValues(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }

  private DocValuesProducer readerToDocValuesProducer(final LeafReader reader) {
    return new DocValuesProducer() {

      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {  
        return reader.getNumericDocValues(field.name);
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return reader.getBinaryDocValues(field.name);
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return reader.getSortedDocValues(field.name);
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return reader.getSortedNumericDocValues(field.name);
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return reader.getSortedSetDocValues(field.name);
      }

      @Override
      public Bits getDocsWithField(FieldInfo field) throws IOException {
        return reader.getDocsWithField(field.name);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }

  private StoredFieldsReader readerToStoredFieldsReader(final LeafReader reader) {
    return new StoredFieldsReader() {
      @Override
      public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
        reader.document(docID, visitor);
      }

      @Override
      public StoredFieldsReader clone() {
        return readerToStoredFieldsReader(reader);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }

  private TermVectorsReader readerToTermVectorsReader(final LeafReader reader) {
    return new TermVectorsReader() {
      @Override
      public Fields get(int docID) throws IOException {
        return reader.getTermVectors(docID);
      }

      @Override
      public TermVectorsReader clone() {
        return readerToTermVectorsReader(reader);
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }

  private FieldsProducer readerToFieldsProducer(final LeafReader reader) throws IOException {
    final Fields fields = reader.fields();
    return new FieldsProducer() {
      @Override
      public Iterator<String> iterator() {
        return fields.iterator();
      }

      @Override
      public Terms terms(String field) throws IOException {
        return fields.terms(field);
      }

      @Override
      public int size() {
        return fields.size();
      }

      @Override
      public void checkIntegrity() throws IOException {
        // We already checkIntegrity the entire reader up front in SegmentMerger
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }

  // NOTE: removes any "all deleted" readers from mergeState.readers
  private void setDocMaps(List<LeafReader> readers) throws IOException {
    final int numReaders = maxDocs.length;

    // Remap docIDs
    int docBase = 0;
    for(int i=0;i<numReaders;i++) {
      final LeafReader reader = readers.get(i);
      this.docBase[i] = docBase;
      final DocMap docMap = DocMap.build(reader);
      docMaps[i] = docMap;
      docBase += docMap.numDocs();
    }

    segmentInfo.setDocCount(docBase);
  }

  /**
   * Class for recording units of work when merging segments.
   */
  public static class CheckAbort {
    private double workCount;
    private final MergePolicy.OneMerge merge;
    private final Directory dir;

    /** Creates a #CheckAbort instance. */
    public CheckAbort(MergePolicy.OneMerge merge, Directory dir) {
      this.merge = merge;
      this.dir = dir;
    }

    /**
     * Records the fact that roughly units amount of work
     * have been done since this method was last called.
     * When adding time-consuming code into SegmentMerger,
     * you should test different values for units to ensure
     * that the time in between calls to merge.checkAborted
     * is up to ~ 1 second.
     */
    public void work(double units) throws MergePolicy.MergeAbortedException {
      workCount += units;
      if (workCount >= 10000.0) {
        merge.checkAborted(dir);
        workCount = 0;
      }
    }

    /** If you use this: IW.close(false) cannot abort your merge!
     * @lucene.internal */
    static final MergeState.CheckAbort NONE = new MergeState.CheckAbort(null, null) {
      @Override
      public void work(double units) {
        // do nothing
      }
    };
  }


  /**
   * Remaps docids around deletes during merge
   */
  public static abstract class DocMap {

    DocMap() {}

    /** Returns the mapped docID corresponding to the provided one. */
    public abstract int get(int docID);

    /** Returns the total number of documents, ignoring
     *  deletions. */
    public abstract int maxDoc();

    /** Returns the number of not-deleted documents. */
    public final int numDocs() {
      return maxDoc() - numDeletedDocs();
    }

    /** Returns the number of deleted documents. */
    public abstract int numDeletedDocs();

    /** Returns true if there are any deletions. */
    public boolean hasDeletions() {
      return numDeletedDocs() > 0;
    }

    /** Creates a {@link DocMap} instance appropriate for
     *  this reader. */
    public static DocMap build(LeafReader reader) {
      final int maxDoc = reader.maxDoc();
      if (!reader.hasDeletions()) {
        return new NoDelDocMap(maxDoc);
      }
      final Bits liveDocs = reader.getLiveDocs();
      return build(maxDoc, liveDocs);
    }

    static DocMap build(final int maxDoc, final Bits liveDocs) {
      assert liveDocs != null;
      final PackedLongValues.Builder docMapBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
      int del = 0;
      for (int i = 0; i < maxDoc; ++i) {
        docMapBuilder.add(i - del);
        if (!liveDocs.get(i)) {
          ++del;
        }
      }
      final PackedLongValues docMap = docMapBuilder.build();
      final int numDeletedDocs = del;
      assert docMap.size() == maxDoc;
      return new DocMap() {

        @Override
        public int get(int docID) {
          if (!liveDocs.get(docID)) {
            return -1;
          }
          return (int) docMap.get(docID);
        }

        @Override
        public int maxDoc() {
          return maxDoc;
        }

        @Override
        public int numDeletedDocs() {
          return numDeletedDocs;
        }
      };
    }
  }

  private static final class NoDelDocMap extends DocMap {

    private final int maxDoc;

    NoDelDocMap(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int get(int docID) {
      return docID;
    }

    @Override
    public int maxDoc() {
      return maxDoc;
    }

    @Override
    public int numDeletedDocs() {
      return 0;
    }
  }
}
