package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldReaderException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;

/**
 * Class responsible for access to stored document fields.
 * <p/>
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 * 
 * @lucene.internal
 */
public final class DefaultFieldsReader extends FieldsReader implements Cloneable, Closeable {
  private final static int FORMAT_SIZE = 4;

  private final FieldInfos fieldInfos;
  private CloseableThreadLocal<IndexInput> fieldsStreamTL = new CloseableThreadLocal<IndexInput>();
  
  // The main fieldStream, used only for cloning.
  private final IndexInput cloneableFieldsStream;

  // This is a clone of cloneableFieldsStream used for reading documents.
  // It should not be cloned outside of a synchronized context.
  private final IndexInput fieldsStream;

  private final IndexInput cloneableIndexStream;
  private final IndexInput indexStream;
  private int numTotalDocs;
  private int size;
  private boolean closed;
  private final int format;

  // The docID offset where our docs begin in the index
  // file.  This will be 0 if we have our own private file.
  private int docStoreOffset;

  private boolean isOriginal = false;

  /** Returns a cloned FieldsReader that shares open
   *  IndexInputs with the original one.  It is the caller's
   *  job not to close the original FieldsReader until all
   *  clones are called (eg, currently SegmentReader manages
   *  this logic). */
  @Override
  public DefaultFieldsReader clone() {
    ensureOpen();
    return new DefaultFieldsReader(fieldInfos, numTotalDocs, size, format, docStoreOffset, cloneableFieldsStream, cloneableIndexStream);
  }

  /** Verifies that the code version which wrote the segment is supported. */
  public static void checkCodeVersion(Directory dir, String segment) throws IOException {
    final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", DefaultFieldsWriter.FIELDS_INDEX_EXTENSION);
    IndexInput idxStream = dir.openInput(indexStreamFN, IOContext.DEFAULT);
    
    try {
      int format = idxStream.readInt();
      if (format < DefaultFieldsWriter.FORMAT_MINIMUM)
        throw new IndexFormatTooOldException(indexStreamFN, format, DefaultFieldsWriter.FORMAT_MINIMUM, DefaultFieldsWriter.FORMAT_CURRENT);
      if (format > DefaultFieldsWriter.FORMAT_CURRENT)
        throw new IndexFormatTooNewException(indexStreamFN, format, DefaultFieldsWriter.FORMAT_MINIMUM, DefaultFieldsWriter.FORMAT_CURRENT);
    } finally {
      idxStream.close();
    }
  
  }
  
  // Used only by clone
  private DefaultFieldsReader(FieldInfos fieldInfos, int numTotalDocs, int size, int format, int docStoreOffset,
                       IndexInput cloneableFieldsStream, IndexInput cloneableIndexStream) {
    this.fieldInfos = fieldInfos;
    this.numTotalDocs = numTotalDocs;
    this.size = size;
    this.format = format;
    this.docStoreOffset = docStoreOffset;
    this.cloneableFieldsStream = cloneableFieldsStream;
    this.cloneableIndexStream = cloneableIndexStream;
    fieldsStream = (IndexInput) cloneableFieldsStream.clone();
    indexStream = (IndexInput) cloneableIndexStream.clone();
  }
  
  public DefaultFieldsReader(Directory d, String segment, FieldInfos fn) throws IOException {
    this(d, segment, fn, IOContext.DEFAULT, -1, 0);
  }

  public DefaultFieldsReader(Directory d, String segment, FieldInfos fn, IOContext context, int docStoreOffset, int size) throws IOException {
    boolean success = false;
    isOriginal = true;
    try {
      fieldInfos = fn;

      cloneableFieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, "", DefaultFieldsWriter.FIELDS_EXTENSION), context);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", DefaultFieldsWriter.FIELDS_INDEX_EXTENSION);
      cloneableIndexStream = d.openInput(indexStreamFN, context);
      
      format = cloneableIndexStream.readInt();

      if (format < DefaultFieldsWriter.FORMAT_MINIMUM)
        throw new IndexFormatTooOldException(indexStreamFN, format, DefaultFieldsWriter.FORMAT_MINIMUM, DefaultFieldsWriter.FORMAT_CURRENT);
      if (format > DefaultFieldsWriter.FORMAT_CURRENT)
        throw new IndexFormatTooNewException(indexStreamFN, format, DefaultFieldsWriter.FORMAT_MINIMUM, DefaultFieldsWriter.FORMAT_CURRENT);

      fieldsStream = (IndexInput) cloneableFieldsStream.clone();

      final long indexSize = cloneableIndexStream.length() - FORMAT_SIZE;
      
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
      }

      indexStream = (IndexInput) cloneableIndexStream.clone();
      numTotalDocs = (int) (indexSize >> 3);
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        close();
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
   * Closes the underlying {@link org.apache.lucene.store.IndexInput} streams, including any ones associated with a
   * lazy implementation of a Field.  This means that the Fields values will not be accessible.
   *
   * @throws IOException
   */
  public final void close() throws IOException {
    if (!closed) {
      if (isOriginal) {
        IOUtils.close(fieldsStream, indexStream, fieldsStreamTL, cloneableFieldsStream, cloneableIndexStream);
      } else {
        IOUtils.close(fieldsStream, indexStream, fieldsStreamTL);
      }
      closed = true;
    }
  }

  public final int size() {
    return size;
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
      assert bits <= (DefaultFieldsWriter.FIELD_IS_NUMERIC_MASK | DefaultFieldsWriter.FIELD_IS_BINARY): "bits=" + Integer.toHexString(bits);

      final boolean binary = (bits & DefaultFieldsWriter.FIELD_IS_BINARY) != 0;
      final int numeric = bits & DefaultFieldsWriter.FIELD_IS_NUMERIC_MASK;

      final boolean doStop;
      if (binary) {
        final int numBytes = fieldsStream.readVInt();
        doStop = visitor.binaryField(fieldInfo, fieldsStream, numBytes);
      } else if (numeric != 0) {
        switch(numeric) {
        case DefaultFieldsWriter.FIELD_IS_NUMERIC_INT:
          doStop = visitor.intField(fieldInfo, fieldsStream.readInt());
          break;
        case DefaultFieldsWriter.FIELD_IS_NUMERIC_LONG:
          doStop = visitor.longField(fieldInfo, fieldsStream.readLong());
          break;
        case DefaultFieldsWriter.FIELD_IS_NUMERIC_FLOAT:
          doStop = visitor.floatField(fieldInfo, Float.intBitsToFloat(fieldsStream.readInt()));
          break;
        case DefaultFieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
          doStop = visitor.doubleField(fieldInfo, Double.longBitsToDouble(fieldsStream.readLong()));
          break;
        default:
          throw new FieldReaderException("Invalid numeric type: " + Integer.toHexString(numeric));
        }
      } else {
        // Text:
        final int numUTF8Bytes = fieldsStream.readVInt();
        doStop = visitor.stringField(fieldInfo, fieldsStream, numUTF8Bytes);
      }

      if (doStop) {
        return;
      }
    }
  }

  /** Returns the length in bytes of each raw document in a
   *  contiguous range of length numDocs starting with
   *  startDocID.  Returns the IndexInput (the fieldStream),
   *  already seeked to the starting point for startDocID.*/
  public final IndexInput rawDocs(int[] lengths, int startDocID, int numDocs) throws IOException {
    seekIndex(startDocID);
    long startOffset = indexStream.readLong();
    long lastOffset = startOffset;
    int count = 0;
    while (count < numDocs) {
      final long offset;
      final int docID = docStoreOffset + startDocID + count + 1;
      assert docID <= numTotalDocs;
      if (docID < numTotalDocs) 
        offset = indexStream.readLong();
      else
        offset = fieldsStream.length();
      lengths[count++] = (int) (offset-lastOffset);
      lastOffset = offset;
    }

    fieldsStream.seek(startOffset);

    return fieldsStream;
  }

  /**
   * Skip the field.  We still have to read some of the information about the field, but can skip past the actual content.
   * This will have the most payoff on large fields.
   */
  private void skipField(int numeric) throws IOException {
    final int numBytes;
    switch(numeric) {
      case 0:
        numBytes = fieldsStream.readVInt();
        break;
      case DefaultFieldsWriter.FIELD_IS_NUMERIC_INT:
      case DefaultFieldsWriter.FIELD_IS_NUMERIC_FLOAT:
        numBytes = 4;
        break;
      case DefaultFieldsWriter.FIELD_IS_NUMERIC_LONG:
      case DefaultFieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
        numBytes = 8;
        break;
      default:
        throw new FieldReaderException("Invalid numeric type: " + Integer.toHexString(numeric));
    }
    
    skipFieldBytes(numBytes);
  }
  
  private void skipFieldBytes(int toRead) throws IOException {
    fieldsStream.seek(fieldsStream.getFilePointer() + toRead);
  }
}
