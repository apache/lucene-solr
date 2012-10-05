package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;

/**
 * Class responsible for access to stored document fields.
 * <p/>
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 * 
 * @deprecated Only for reading existing 3.x indexes
 */
@Deprecated
final class Lucene3xStoredFieldsReader extends StoredFieldsReader implements Cloneable, Closeable {
  private final static int FORMAT_SIZE = 4;

  /** Extension of stored fields file */
  public static final String FIELDS_EXTENSION = "fdt";
  
  /** Extension of stored fields index file */
  public static final String FIELDS_INDEX_EXTENSION = "fdx";
  
  // Lucene 3.0: Removal of compressed fields
  static final int FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS = 2;

  // Lucene 3.2: NumericFields are stored in binary format
  static final int FORMAT_LUCENE_3_2_NUMERIC_FIELDS = 3;

  // NOTE: if you introduce a new format, make it 1 higher
  // than the current one, and always change this if you
  // switch to a new format!
  public static final int FORMAT_CURRENT = FORMAT_LUCENE_3_2_NUMERIC_FIELDS;

  // when removing support for old versions, leave the last supported version here
  static final int FORMAT_MINIMUM = FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;
  
  // NOTE: bit 0 is free here!  You can steal it!
  public static final int FIELD_IS_BINARY = 1 << 1;

  // the old bit 1 << 2 was compressed, is now left out

  private static final int _NUMERIC_BIT_SHIFT = 3;
  static final int FIELD_IS_NUMERIC_MASK = 0x07 << _NUMERIC_BIT_SHIFT;

  public static final int FIELD_IS_NUMERIC_INT = 1 << _NUMERIC_BIT_SHIFT;
  public static final int FIELD_IS_NUMERIC_LONG = 2 << _NUMERIC_BIT_SHIFT;
  public static final int FIELD_IS_NUMERIC_FLOAT = 3 << _NUMERIC_BIT_SHIFT;
  public static final int FIELD_IS_NUMERIC_DOUBLE = 4 << _NUMERIC_BIT_SHIFT;

  private final FieldInfos fieldInfos;
  private final IndexInput fieldsStream;
  private final IndexInput indexStream;
  private int numTotalDocs;
  private int size;
  private boolean closed;
  private final int format;

  // The docID offset where our docs begin in the index
  // file.  This will be 0 if we have our own private file.
  private int docStoreOffset;
  
  // when we are inside a compound share doc store (CFX),
  // (lucene 3.0 indexes only), we privately open our own fd.
  private final CompoundFileDirectory storeCFSReader;

  /** Returns a cloned FieldsReader that shares open
   *  IndexInputs with the original one.  It is the caller's
   *  job not to close the original FieldsReader until all
   *  clones are called (eg, currently SegmentReader manages
   *  this logic). */
  @Override
  public Lucene3xStoredFieldsReader clone() {
    ensureOpen();
    return new Lucene3xStoredFieldsReader(fieldInfos, numTotalDocs, size, format, docStoreOffset, fieldsStream.clone(), indexStream.clone());
  }

  /** Verifies that the code version which wrote the segment is supported. */
  public static void checkCodeVersion(Directory dir, String segment) throws IOException {
    final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", FIELDS_INDEX_EXTENSION);
    IndexInput idxStream = dir.openInput(indexStreamFN, IOContext.DEFAULT);
    
    try {
      int format = idxStream.readInt();
      if (format < FORMAT_MINIMUM)
        throw new IndexFormatTooOldException(idxStream, format, FORMAT_MINIMUM, FORMAT_CURRENT);
      if (format > FORMAT_CURRENT)
        throw new IndexFormatTooNewException(idxStream, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    } finally {
      idxStream.close();
    }
  }
  
  // Used only by clone
  private Lucene3xStoredFieldsReader(FieldInfos fieldInfos, int numTotalDocs, int size, int format, int docStoreOffset,
                       IndexInput fieldsStream, IndexInput indexStream) {
    this.fieldInfos = fieldInfos;
    this.numTotalDocs = numTotalDocs;
    this.size = size;
    this.format = format;
    this.docStoreOffset = docStoreOffset;
    this.fieldsStream = fieldsStream;
    this.indexStream = indexStream;
    this.storeCFSReader = null;
  }

  public Lucene3xStoredFieldsReader(Directory d, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    final String segment = Lucene3xSegmentInfoFormat.getDocStoreSegment(si);
    final int docStoreOffset = Lucene3xSegmentInfoFormat.getDocStoreOffset(si);
    final int size = si.getDocCount();
    boolean success = false;
    fieldInfos = fn;
    try {
      if (docStoreOffset != -1 && Lucene3xSegmentInfoFormat.getDocStoreIsCompoundFile(si)) {
        d = storeCFSReader = new CompoundFileDirectory(si.dir, 
            IndexFileNames.segmentFileName(segment, "", Lucene3xCodec.COMPOUND_FILE_STORE_EXTENSION), context, false);
      } else {
        storeCFSReader = null;
      }
      fieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION), context);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", FIELDS_INDEX_EXTENSION);
      indexStream = d.openInput(indexStreamFN, context);
      
      format = indexStream.readInt();

      if (format < FORMAT_MINIMUM)
        throw new IndexFormatTooOldException(indexStream, format, FORMAT_MINIMUM, FORMAT_CURRENT);
      if (format > FORMAT_CURRENT)
        throw new IndexFormatTooNewException(indexStream, format, FORMAT_MINIMUM, FORMAT_CURRENT);

      final long indexSize = indexStream.length() - FORMAT_SIZE;
      
      if (docStoreOffset != -1) {
        // We read only a slice out of this shared fields file
        this.docStoreOffset = docStoreOffset;
        this.size = size;

        // Verify the file is long enough to hold all of our
        // docs
        assert ((int) (indexSize / 8)) >= size + this.docStoreOffset: "indexSize=" + indexSize + " size=" + size + " docStoreOffset=" + docStoreOffset;
      } else {
        this.docStoreOffset = 0;
        this.size = (int) (indexSize >> 3);
        // Verify two sources of "maxDoc" agree:
        if (this.size != si.getDocCount()) {
          throw new CorruptIndexException("doc counts differ for segment " + segment + ": fieldsReader shows " + this.size + " but segmentInfo shows " + si.getDocCount());
        }
      }
      numTotalDocs = (int) (indexSize >> 3);
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        try {
          close();
        } catch (Throwable t) {} // keep our original exception
      }
    }
  }

  /**
   * @throws AlreadyClosedException if this FieldsReader is closed
   */
  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
  }

  /**
   * Closes the underlying {@link org.apache.lucene.store.IndexInput} streams.
   * This means that the Fields values will not be accessible.
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public final void close() throws IOException {
    if (!closed) {
      IOUtils.close(fieldsStream, indexStream, storeCFSReader);
      closed = true;
    }
  }

  private void seekIndex(int docID) throws IOException {
    indexStream.seek(FORMAT_SIZE + (docID + docStoreOffset) * 8L);
  }

  public final void visitDocument(int n, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    seekIndex(n);
    fieldsStream.seek(indexStream.readLong());

    final int numFields = fieldsStream.readVInt();
    for (int fieldIDX = 0; fieldIDX < numFields; fieldIDX++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
      
      int bits = fieldsStream.readByte() & 0xFF;
      assert bits <= (FIELD_IS_NUMERIC_MASK | FIELD_IS_BINARY): "bits=" + Integer.toHexString(bits);

      switch(visitor.needsField(fieldInfo)) {
        case YES:
          readField(visitor, fieldInfo, bits);
          break;
        case NO: 
          skipField(bits);
          break;
        case STOP: 
          return;
      }
    }
  }

  private void readField(StoredFieldVisitor visitor, FieldInfo info, int bits) throws IOException {
    final int numeric = bits & FIELD_IS_NUMERIC_MASK;
    if (numeric != 0) {
      switch(numeric) {
        case FIELD_IS_NUMERIC_INT:
          visitor.intField(info, fieldsStream.readInt());
          return;
        case FIELD_IS_NUMERIC_LONG:
          visitor.longField(info, fieldsStream.readLong());
          return;
        case FIELD_IS_NUMERIC_FLOAT:
          visitor.floatField(info, Float.intBitsToFloat(fieldsStream.readInt()));
          return;
        case FIELD_IS_NUMERIC_DOUBLE:
          visitor.doubleField(info, Double.longBitsToDouble(fieldsStream.readLong()));
          return;
        default:
          throw new CorruptIndexException("Invalid numeric type: " + Integer.toHexString(numeric));
      }
    } else { 
      final int length = fieldsStream.readVInt();
      byte bytes[] = new byte[length];
      fieldsStream.readBytes(bytes, 0, length);
      if ((bits & FIELD_IS_BINARY) != 0) {
        visitor.binaryField(info, bytes);
      } else {
        visitor.stringField(info, new String(bytes, 0, bytes.length, IOUtils.CHARSET_UTF_8));
      }
    }
  }
  
  private void skipField(int bits) throws IOException {
    final int numeric = bits & FIELD_IS_NUMERIC_MASK;
    if (numeric != 0) {
      switch(numeric) {
        case FIELD_IS_NUMERIC_INT:
        case FIELD_IS_NUMERIC_FLOAT:
          fieldsStream.readInt();
          return;
        case FIELD_IS_NUMERIC_LONG:
        case FIELD_IS_NUMERIC_DOUBLE:
          fieldsStream.readLong();
          return;
        default: 
          throw new CorruptIndexException("Invalid numeric type: " + Integer.toHexString(numeric));
      }
    } else {
      final int length = fieldsStream.readVInt();
      fieldsStream.seek(fieldsStream.getFilePointer() + length);
    }
  }
}
