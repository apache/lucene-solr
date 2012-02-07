package org.apache.lucene.codecs.lucene40;

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

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.util.Set;

/**
 * Class responsible for access to stored document fields.
 * <p/>
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 * 
 * @lucene.internal
 */
public final class Lucene40StoredFieldsReader extends StoredFieldsReader implements Cloneable, Closeable {
  private final static int FORMAT_SIZE = 4;

  private final FieldInfos fieldInfos;
  private final IndexInput fieldsStream;
  private final IndexInput indexStream;
  private int numTotalDocs;
  private int size;
  private boolean closed;

  /** Returns a cloned FieldsReader that shares open
   *  IndexInputs with the original one.  It is the caller's
   *  job not to close the original FieldsReader until all
   *  clones are called (eg, currently SegmentReader manages
   *  this logic). */
  @Override
  public Lucene40StoredFieldsReader clone() {
    ensureOpen();
    return new Lucene40StoredFieldsReader(fieldInfos, numTotalDocs, size, (IndexInput)fieldsStream.clone(), (IndexInput)indexStream.clone());
  }
  
  // Used only by clone
  private Lucene40StoredFieldsReader(FieldInfos fieldInfos, int numTotalDocs, int size, IndexInput fieldsStream, IndexInput indexStream) {
    this.fieldInfos = fieldInfos;
    this.numTotalDocs = numTotalDocs;
    this.size = size;
    this.fieldsStream = fieldsStream;
    this.indexStream = indexStream;
  }

  public Lucene40StoredFieldsReader(Directory d, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    try {
      fieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, "", Lucene40StoredFieldsWriter.FIELDS_EXTENSION), context);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, "", Lucene40StoredFieldsWriter.FIELDS_INDEX_EXTENSION);
      indexStream = d.openInput(indexStreamFN, context);
      
      // its a 4.0 codec: so its not too-old, its corrupt.
      // TODO: change this to CodecUtil.checkHeader
      if (Lucene40StoredFieldsWriter.FORMAT_CURRENT != indexStream.readInt()) {
        throw new CorruptIndexException("unexpected fdx header: " + indexStream);
      }

      final long indexSize = indexStream.length() - FORMAT_SIZE;
      this.size = (int) (indexSize >> 3);
      // Verify two sources of "maxDoc" agree:
      if (this.size != si.docCount) {
        throw new CorruptIndexException("doc counts differ for segment " + segment + ": fieldsReader shows " + this.size + " but segmentInfo shows " + si.docCount);
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
   * Closes the underlying {@link org.apache.lucene.store.IndexInput} streams.
   * This means that the Fields values will not be accessible.
   *
   * @throws IOException
   */
  public final void close() throws IOException {
    if (!closed) {
      IOUtils.close(fieldsStream, indexStream);
      closed = true;
    }
  }

  public final int size() {
    return size;
  }

  private void seekIndex(int docID) throws IOException {
    indexStream.seek(FORMAT_SIZE + docID * 8L);
  }

  public final void visitDocument(int n, StoredFieldVisitor visitor) throws CorruptIndexException, IOException {
    seekIndex(n);
    fieldsStream.seek(indexStream.readLong());

    final int numFields = fieldsStream.readVInt();
    for (int fieldIDX = 0; fieldIDX < numFields; fieldIDX++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
      
      int bits = fieldsStream.readByte() & 0xFF;
      assert bits <= (Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_MASK | Lucene40StoredFieldsWriter.FIELD_IS_BINARY): "bits=" + Integer.toHexString(bits);

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
    final int numeric = bits & Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_MASK;
    if (numeric != 0) {
      switch(numeric) {
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_INT:
          visitor.intField(info, fieldsStream.readInt());
          return;
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_LONG:
          visitor.longField(info, fieldsStream.readLong());
          return;
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_FLOAT:
          visitor.floatField(info, Float.intBitsToFloat(fieldsStream.readInt()));
          return;
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
          visitor.doubleField(info, Double.longBitsToDouble(fieldsStream.readLong()));
          return;
        default:
          throw new CorruptIndexException("Invalid numeric type: " + Integer.toHexString(numeric));
      }
    } else { 
      final int length = fieldsStream.readVInt();
      byte bytes[] = new byte[length];
      fieldsStream.readBytes(bytes, 0, length);
      if ((bits & Lucene40StoredFieldsWriter.FIELD_IS_BINARY) != 0) {
        visitor.binaryField(info, bytes, 0, bytes.length);
      } else {
        visitor.stringField(info, new String(bytes, 0, bytes.length, IOUtils.CHARSET_UTF_8));
      }
    }
  }
  
  private void skipField(int bits) throws IOException {
    final int numeric = bits & Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_MASK;
    if (numeric != 0) {
      switch(numeric) {
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_INT:
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_FLOAT:
          fieldsStream.readInt();
          return;
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_LONG:
        case Lucene40StoredFieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
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
      final int docID = startDocID + count + 1;
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

  public static void files(SegmentInfo info, Set<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, "", Lucene40StoredFieldsWriter.FIELDS_INDEX_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, "", Lucene40StoredFieldsWriter.FIELDS_EXTENSION));
  }
}
