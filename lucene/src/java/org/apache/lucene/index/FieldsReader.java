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

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.zip.DataFormatException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.AbstractField;
import org.apache.lucene.document.CompressionTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

/**
 * Class responsible for access to stored document fields.
 * <p/>
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 */
final class FieldsReader implements Cloneable, Closeable {
  private final FieldInfos fieldInfos;

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
  private final int formatSize;

  // The docID offset where our docs begin in the index
  // file.  This will be 0 if we have our own private file.
  private int docStoreOffset;

  private CloseableThreadLocal<IndexInput> fieldsStreamTL = new CloseableThreadLocal<IndexInput>();
  private boolean isOriginal = false;

  /** Returns a cloned FieldsReader that shares open
   *  IndexInputs with the original one.  It is the caller's
   *  job not to close the original FieldsReader until all
   *  clones are called (eg, currently SegmentReader manages
   *  this logic). */
  @Override
  public Object clone() {
    ensureOpen();
    return new FieldsReader(fieldInfos, numTotalDocs, size, format, formatSize, docStoreOffset, cloneableFieldsStream, cloneableIndexStream);
  }

  /**
   * Detects the code version this segment was written with. Returns either
   * "2.x" for all pre-3.0 segments, or "3.0" for 3.0 segments. This method
   * should not be called for 3.1+ segments since they already record their code
   * version.
   */
  static String detectCodeVersion(Directory dir, String segment) throws IOException {
    IndexInput idxStream = dir.openInput(IndexFileNames.segmentFileName(segment, IndexFileNames.FIELDS_INDEX_EXTENSION), 1024);
    try {
      int format = idxStream.readInt();
      if (format < FieldsWriter.FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS) {
        return "2.x";
      } else {
        return "3.0";
      }
    } finally {
      idxStream.close();
    }
  }
  
  // Used only by clone
  private FieldsReader(FieldInfos fieldInfos, int numTotalDocs, int size, int format, int formatSize,
                       int docStoreOffset, IndexInput cloneableFieldsStream, IndexInput cloneableIndexStream) {
    this.fieldInfos = fieldInfos;
    this.numTotalDocs = numTotalDocs;
    this.size = size;
    this.format = format;
    this.formatSize = formatSize;
    this.docStoreOffset = docStoreOffset;
    this.cloneableFieldsStream = cloneableFieldsStream;
    this.cloneableIndexStream = cloneableIndexStream;
    fieldsStream = (IndexInput) cloneableFieldsStream.clone();
    indexStream = (IndexInput) cloneableIndexStream.clone();
  }
  
  FieldsReader(Directory d, String segment, FieldInfos fn) throws IOException {
    this(d, segment, fn, BufferedIndexInput.BUFFER_SIZE, -1, 0);
  }

  FieldsReader(Directory d, String segment, FieldInfos fn, int readBufferSize) throws IOException {
    this(d, segment, fn, readBufferSize, -1, 0);
  }

  FieldsReader(Directory d, String segment, FieldInfos fn, int readBufferSize, int docStoreOffset, int size) throws IOException {
    boolean success = false;
    isOriginal = true;
    try {
      fieldInfos = fn;

      cloneableFieldsStream = d.openInput(IndexFileNames.segmentFileName(segment, IndexFileNames.FIELDS_EXTENSION), readBufferSize);
      final String indexStreamFN = IndexFileNames.segmentFileName(segment, IndexFileNames.FIELDS_INDEX_EXTENSION);
      cloneableIndexStream = d.openInput(indexStreamFN, readBufferSize);
      
      // First version of fdx did not include a format
      // header, but, the first int will always be 0 in that
      // case
      int firstInt = cloneableIndexStream.readInt();
      if (firstInt == 0)
        format = 0;
      else
        format = firstInt;

      if (format > FieldsWriter.FORMAT_CURRENT)
        throw new IndexFormatTooNewException(cloneableIndexStream, format, 0, FieldsWriter.FORMAT_CURRENT);

      if (format > FieldsWriter.FORMAT)
        formatSize = 4;
      else
        formatSize = 0;

      if (format < FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES)
        cloneableFieldsStream.setModifiedUTF8StringsMode();

      fieldsStream = (IndexInput) cloneableFieldsStream.clone();

      final long indexSize = cloneableIndexStream.length()-formatSize;
      
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

  final int size() {
    return size;
  }

  private final void seekIndex(int docID) throws IOException {
    indexStream.seek(formatSize + (docID + docStoreOffset) * 8L);
  }

  boolean canReadRawDocs() {
    // Disable reading raw docs in 2.x format, because of the removal of compressed
    // fields in 3.0. We don't want rawDocs() to decode field bits to figure out
    // if a field was compressed, hence we enforce ordinary (non-raw) stored field merges
    // for <3.0 indexes.
    return format >= FieldsWriter.FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS;
  }

  final Document doc(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    seekIndex(n);
    long position = indexStream.readLong();
    fieldsStream.seek(position);

    Document doc = new Document();
    int numFields = fieldsStream.readVInt();
    out: for (int i = 0; i < numFields; i++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fi = fieldInfos.fieldInfo(fieldNumber);
      FieldSelectorResult acceptField = fieldSelector == null ? FieldSelectorResult.LOAD : fieldSelector.accept(fi.name);
      
      int bits = fieldsStream.readByte() & 0xFF;
      assert bits <= (FieldsWriter.FIELD_IS_NUMERIC_MASK | FieldsWriter.FIELD_IS_COMPRESSED | FieldsWriter.FIELD_IS_TOKENIZED | FieldsWriter.FIELD_IS_BINARY): "bits=" + Integer.toHexString(bits);

      boolean compressed = (bits & FieldsWriter.FIELD_IS_COMPRESSED) != 0;
      assert (compressed ? (format < FieldsWriter.FORMAT_LUCENE_3_0_NO_COMPRESSED_FIELDS) : true)
        : "compressed fields are only allowed in indexes of version <= 2.9";
      boolean tokenize = (bits & FieldsWriter.FIELD_IS_TOKENIZED) != 0;
      boolean binary = (bits & FieldsWriter.FIELD_IS_BINARY) != 0;
      final int numeric = bits & FieldsWriter.FIELD_IS_NUMERIC_MASK;

      switch (acceptField) {
        case LOAD:
          addField(doc, fi, binary, compressed, tokenize, numeric);
          break;
        case LOAD_AND_BREAK:
          addField(doc, fi, binary, compressed, tokenize, numeric);
          break out; //Get out of this loop
        case LAZY_LOAD:
          addFieldLazy(doc, fi, binary, compressed, tokenize, true, numeric);
          break;
        case LATENT:
          addFieldLazy(doc, fi, binary, compressed, tokenize, false, numeric);
          break;
        case SIZE:
          skipFieldBytes(binary, compressed, addFieldSize(doc, fi, binary, compressed, numeric));
          break;
        case SIZE_AND_BREAK:
          addFieldSize(doc, fi, binary, compressed, numeric);
          break out; //Get out of this loop
        default:
          skipField(binary, compressed, numeric);
      }
    }

    return doc;
  }

  /** Returns the length in bytes of each raw document in a
   *  contiguous range of length numDocs starting with
   *  startDocID.  Returns the IndexInput (the fieldStream),
   *  already seeked to the starting point for startDocID.*/
  final IndexInput rawDocs(int[] lengths, int startDocID, int numDocs) throws IOException {
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
  private void skipField(boolean binary, boolean compressed, int numeric) throws IOException {
    final int numBytes;
    switch(numeric) {
      case 0:
        numBytes = fieldsStream.readVInt();
        break;
      case FieldsWriter.FIELD_IS_NUMERIC_INT:
      case FieldsWriter.FIELD_IS_NUMERIC_FLOAT:
        numBytes = 4;
        break;
      case FieldsWriter.FIELD_IS_NUMERIC_LONG:
      case FieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
        numBytes = 8;
        break;
      default:
        throw new FieldReaderException("Invalid numeric type: " + Integer.toHexString(numeric));
    }
    
    skipFieldBytes(binary, compressed, numBytes);
  }
  
  private void skipFieldBytes(boolean binary, boolean compressed, int toRead) throws IOException {
    if (format >= FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES || binary || compressed) {
      fieldsStream.seek(fieldsStream.getFilePointer() + toRead);
    } else {
      // We need to skip chars.  This will slow us down, but still better
      fieldsStream.skipChars(toRead);
    }
  }

  private NumericField loadNumericField(FieldInfo fi, int numeric) throws IOException {
    assert numeric != 0;
    switch(numeric) {
      case FieldsWriter.FIELD_IS_NUMERIC_INT:
        return new NumericField(fi.name, Field.Store.YES, fi.isIndexed).setIntValue(fieldsStream.readInt());
      case FieldsWriter.FIELD_IS_NUMERIC_LONG:
        return new NumericField(fi.name, Field.Store.YES, fi.isIndexed).setLongValue(fieldsStream.readLong());
      case FieldsWriter.FIELD_IS_NUMERIC_FLOAT:
        return new NumericField(fi.name, Field.Store.YES, fi.isIndexed).setFloatValue(Float.intBitsToFloat(fieldsStream.readInt()));
      case FieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
        return new NumericField(fi.name, Field.Store.YES, fi.isIndexed).setDoubleValue(Double.longBitsToDouble(fieldsStream.readLong()));
      default:
        throw new FieldReaderException("Invalid numeric type: " + Integer.toHexString(numeric));
    }
  }

  private void addFieldLazy(Document doc, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize, boolean cacheResult, int numeric) throws IOException {
    final AbstractField f;
    if (binary) {
      int toRead = fieldsStream.readVInt();
      long pointer = fieldsStream.getFilePointer();
      f = new LazyField(fi.name, Field.Store.YES, toRead, pointer, binary, compressed, cacheResult);
      //Need to move the pointer ahead by toRead positions
      fieldsStream.seek(pointer + toRead);
    } else if (numeric != 0) {
      f = loadNumericField(fi, numeric);
    } else {
      Field.Store store = Field.Store.YES;
      Field.Index index = Field.Index.toIndex(fi.isIndexed, tokenize);
      Field.TermVector termVector = Field.TermVector.toTermVector(fi.storeTermVector, fi.storeOffsetWithTermVector, fi.storePositionWithTermVector);

      if (compressed) {
        int toRead = fieldsStream.readVInt();
        long pointer = fieldsStream.getFilePointer();
        f = new LazyField(fi.name, store, toRead, pointer, binary, compressed, cacheResult);
        //skip over the part that we aren't loading
        fieldsStream.seek(pointer + toRead);
      } else {
        int length = fieldsStream.readVInt();
        long pointer = fieldsStream.getFilePointer();
        //Skip ahead of where we are by the length of what is stored
        if (format >= FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES) {
          fieldsStream.seek(pointer+length);
        } else {
          fieldsStream.skipChars(length);
        }
        f = new LazyField(fi.name, store, index, termVector, length, pointer, binary, compressed, cacheResult);
      }  
    }
    
    f.setOmitNorms(fi.omitNorms);
    f.setIndexOptions(fi.indexOptions);
    doc.add(f);
  }

  private void addField(Document doc, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize, int numeric) throws CorruptIndexException, IOException {
    final AbstractField f;

    //we have a binary stored field, and it may be compressed
    if (binary) {
      int toRead = fieldsStream.readVInt();
      final byte[] b = new byte[toRead];
      fieldsStream.readBytes(b, 0, b.length);
      if (compressed) {
        f = new Field(fi.name, uncompress(b));
      } else {
        f = new Field(fi.name, b);
      }
    } else if (numeric != 0) {
      f = loadNumericField(fi, numeric);
    } else {
      Field.Store store = Field.Store.YES;
      Field.Index index = Field.Index.toIndex(fi.isIndexed, tokenize);
      Field.TermVector termVector = Field.TermVector.toTermVector(fi.storeTermVector, fi.storeOffsetWithTermVector, fi.storePositionWithTermVector);
      if (compressed) {
        int toRead = fieldsStream.readVInt();
        final byte[] b = new byte[toRead];
        fieldsStream.readBytes(b, 0, b.length);
        f = new Field(fi.name,      // field name
                false,
                new String(uncompress(b), "UTF-8"), // uncompress the value and add as string
                store,
                index,
                termVector);
      } else {
        f = new Field(fi.name,     // name
         false,
                fieldsStream.readString(), // read value
                store,
                index,
                termVector);
      }
    }
    
    f.setIndexOptions(fi.indexOptions);
    f.setOmitNorms(fi.omitNorms);
    doc.add(f);
  }
  
  // Add the size of field as a byte[] containing the 4 bytes of the integer byte size (high order byte first; char = 2 bytes)
  // Read just the size -- caller must skip the field content to continue reading fields
  // Return the size in bytes or chars, depending on field type
  private int addFieldSize(Document doc, FieldInfo fi, boolean binary, boolean compressed, int numeric) throws IOException {
    final int bytesize, size;
    switch(numeric) {
      case 0:
        size = fieldsStream.readVInt();
        bytesize = (binary || compressed) ? size : 2*size;
        break;
      case FieldsWriter.FIELD_IS_NUMERIC_INT:
      case FieldsWriter.FIELD_IS_NUMERIC_FLOAT:
        size = bytesize = 4;
        break;
      case FieldsWriter.FIELD_IS_NUMERIC_LONG:
      case FieldsWriter.FIELD_IS_NUMERIC_DOUBLE:
        size = bytesize = 8;
        break;
      default:
        throw new FieldReaderException("Invalid numeric type: " + Integer.toHexString(numeric));
    }
    byte[] sizebytes = new byte[4];
    sizebytes[0] = (byte) (bytesize>>>24);
    sizebytes[1] = (byte) (bytesize>>>16);
    sizebytes[2] = (byte) (bytesize>>> 8);
    sizebytes[3] = (byte)  bytesize      ;
    doc.add(new Field(fi.name, sizebytes));
    return size;
  }

  /**
   * A Lazy implementation of Fieldable that defers loading of fields until asked for, instead of when the Document is
   * loaded.
   */
  private class LazyField extends AbstractField implements Fieldable {
    private int toRead;
    private long pointer;
    /** @deprecated Only kept for backward-compatbility with <3.0 indexes. Will be removed in 4.0. */
    @Deprecated
    private boolean isCompressed;
	private boolean cacheResult;

    public LazyField(String name, Field.Store store, int toRead, long pointer, boolean isBinary, boolean isCompressed, boolean cacheResult) {
      super(name, store, Field.Index.NO, Field.TermVector.NO);
      this.toRead = toRead;
      this.pointer = pointer;
      this.isBinary = isBinary;
	  this.cacheResult = cacheResult;
      if (isBinary)
        binaryLength = toRead;
      lazy = true;
      this.isCompressed = isCompressed;
    }

    public LazyField(String name, Field.Store store, Field.Index index, Field.TermVector termVector, int toRead, long pointer, boolean isBinary, boolean isCompressed, boolean cacheResult) {
      super(name, store, index, termVector);
      this.toRead = toRead;
      this.pointer = pointer;
      this.isBinary = isBinary;
	  this.cacheResult = cacheResult;
      if (isBinary)
        binaryLength = toRead;
      lazy = true;
      this.isCompressed = isCompressed;
    }

    private IndexInput getFieldStream() {
      IndexInput localFieldsStream = fieldsStreamTL.get();
      if (localFieldsStream == null) {
        localFieldsStream = (IndexInput) cloneableFieldsStream.clone();
        fieldsStreamTL.set(localFieldsStream);
      }
      return localFieldsStream;
    }

    /** The value of the field as a Reader, or null.  If null, the String value,
     * binary value, or TokenStream value is used.  Exactly one of stringValue(), 
     * readerValue(), getBinaryValue(), and tokenStreamValue() must be set. */
    public Reader readerValue() {
      ensureOpen();
      return null;
    }

    /** The value of the field as a TokenStream, or null.  If null, the Reader value,
     * String value, or binary value is used. Exactly one of stringValue(), 
     * readerValue(), getBinaryValue(), and tokenStreamValue() must be set. */
    public TokenStream tokenStreamValue() {
      ensureOpen();
      return null;
    }

    /** The value of the field as a String, or null.  If null, the Reader value,
     * binary value, or TokenStream value is used.  Exactly one of stringValue(), 
     * readerValue(), getBinaryValue(), and tokenStreamValue() must be set. */
    public String stringValue() {
      ensureOpen();
      if (isBinary)
        return null;
      else {
        if (fieldsData == null) {
          IndexInput localFieldsStream = getFieldStream();
		  String value;
          try {
            localFieldsStream.seek(pointer);
            if (isCompressed) {
              final byte[] b = new byte[toRead];
              localFieldsStream.readBytes(b, 0, b.length);
              value = new String(uncompress(b), "UTF-8");
            } else {
              if (format >= FieldsWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES) {
                byte[] bytes = new byte[toRead];
                localFieldsStream.readBytes(bytes, 0, toRead);
                value = new String(bytes, "UTF-8");
              } else {
                //read in chars b/c we already know the length we need to read
                char[] chars = new char[toRead];
                localFieldsStream.readChars(chars, 0, toRead);
                value = new String(chars);
              }
            }
          } catch (IOException e) {
            throw new FieldReaderException(e);
          }
          if (cacheResult){
            fieldsData = value;
          }
          return value;
        } else{
          return (String) fieldsData;
        }
        
      }
    }

    public long getPointer() {
      ensureOpen();
      return pointer;
    }

    public void setPointer(long pointer) {
      ensureOpen();
      this.pointer = pointer;
    }

    public int getToRead() {
      ensureOpen();
      return toRead;
    }

    public void setToRead(int toRead) {
      ensureOpen();
      this.toRead = toRead;
    }

    @Override
    public byte[] getBinaryValue(byte[] result) {
      ensureOpen();

      if (isBinary) {
        if (fieldsData == null) {
          // Allocate new buffer if result is null or too small
          final byte[] b;
		  byte[] value;
          if (result == null || result.length < toRead)
            b = new byte[toRead];
          else
            b = result;
   
          IndexInput localFieldsStream = getFieldStream();

          // Throw this IOException since IndexReader.document does so anyway, so probably not that big of a change for people
          // since they are already handling this exception when getting the document
          try {
            localFieldsStream.seek(pointer);
            localFieldsStream.readBytes(b, 0, toRead);
            if (isCompressed == true) {
              value = uncompress(b);
            } else {
              value = b;
            }
          } catch (IOException e) {
            throw new FieldReaderException(e);
          }

          binaryOffset = 0;
          binaryLength = toRead;
          if (cacheResult == true){
            fieldsData = value;
          }
          return value;
        } else{
          return (byte[]) fieldsData;
        }
      } else {
        return null;
      }
    }
  }

  private byte[] uncompress(byte[] b)
          throws CorruptIndexException {
    try {
      return CompressionTools.decompress(b);
    } catch (DataFormatException e) {
      // this will happen if the field is not compressed
      CorruptIndexException newException = new CorruptIndexException("field data are in wrong format: " + e.toString());
      newException.initCause(e);
      throw newException;
    }
  }
}
