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

import org.apache.lucene.document.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Class responsible for access to stored document fields.
 * <p/>
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 *
 * @version $Id$
 */
final class FieldsReader {
  private final FieldInfos fieldInfos;

  // The main fieldStream, used only for cloning.
  private final IndexInput cloneableFieldsStream;

  // This is a clone of cloneableFieldsStream used for reading documents.
  // It should not be cloned outside of a synchronized context.
  private final IndexInput fieldsStream;

  private final IndexInput indexStream;
  private int size;

  private ThreadLocal fieldsStreamTL = new ThreadLocal();

  FieldsReader(Directory d, String segment, FieldInfos fn) throws IOException {
    fieldInfos = fn;

    cloneableFieldsStream = d.openInput(segment + ".fdt");
    fieldsStream = (IndexInput)cloneableFieldsStream.clone();
    indexStream = d.openInput(segment + ".fdx");
    size = (int) (indexStream.length() / 8);
  }

  /**
   * Closes the underlying {@link org.apache.lucene.store.IndexInput} streams, including any ones associated with a
   * lazy implementation of a Field.  This means that the Fields values will not be accessible.
   *
   * @throws IOException
   */
  final void close() throws IOException {
    fieldsStream.close();
    cloneableFieldsStream.close();
    indexStream.close();
    IndexInput localFieldsStream = (IndexInput) fieldsStreamTL.get();
    if (localFieldsStream != null) {
      localFieldsStream.close();
      fieldsStreamTL.set(null);
    }
  }

  final int size() {
    return size;
  }

  final Document doc(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
    indexStream.seek(n * 8L);
    long position = indexStream.readLong();
    fieldsStream.seek(position);

    Document doc = new Document();
    int numFields = fieldsStream.readVInt();
    for (int i = 0; i < numFields; i++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fi = fieldInfos.fieldInfo(fieldNumber);
      FieldSelectorResult acceptField = fieldSelector == null ? FieldSelectorResult.LOAD : fieldSelector.accept(fi.name);
      
      byte bits = fieldsStream.readByte();
      boolean compressed = (bits & FieldsWriter.FIELD_IS_COMPRESSED) != 0;
      boolean tokenize = (bits & FieldsWriter.FIELD_IS_TOKENIZED) != 0;
      boolean binary = (bits & FieldsWriter.FIELD_IS_BINARY) != 0;
      //TODO: Find an alternative approach here if this list continues to grow beyond the
      //list of 5 or 6 currently here.  See Lucene 762 for discussion
      if (acceptField.equals(FieldSelectorResult.LOAD)) {
        addField(doc, fi, binary, compressed, tokenize);
      }
      else if (acceptField.equals(FieldSelectorResult.LOAD_FOR_MERGE)) {
        addFieldForMerge(doc, fi, binary, compressed, tokenize);
      }
      else if (acceptField.equals(FieldSelectorResult.LOAD_AND_BREAK)){
        addField(doc, fi, binary, compressed, tokenize);
        break;//Get out of this loop
      }
      else if (acceptField.equals(FieldSelectorResult.LAZY_LOAD)) {
        addFieldLazy(doc, fi, binary, compressed, tokenize);
      }
      else if (acceptField.equals(FieldSelectorResult.SIZE)){
        skipField(binary, compressed, addFieldSize(doc, fi, binary, compressed));
      }
      else if (acceptField.equals(FieldSelectorResult.SIZE_AND_BREAK)){
        addFieldSize(doc, fi, binary, compressed);
        break;
      }
      else {
        skipField(binary, compressed);
      }
    }

    return doc;
  }

  /**
   * Skip the field.  We still have to read some of the information about the field, but can skip past the actual content.
   * This will have the most payoff on large fields.
   */
  private void skipField(boolean binary, boolean compressed) throws IOException {
    skipField(binary, compressed, fieldsStream.readVInt());
  }
  
  private void skipField(boolean binary, boolean compressed, int toRead) throws IOException {
    if (binary || compressed) {
      long pointer = fieldsStream.getFilePointer();
      fieldsStream.seek(pointer + toRead);
    } else {
      //We need to skip chars.  This will slow us down, but still better
      fieldsStream.skipChars(toRead);
    }
  }

  private void addFieldLazy(Document doc, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize) throws IOException {
    if (binary == true) {
      int toRead = fieldsStream.readVInt();
      long pointer = fieldsStream.getFilePointer();
      if (compressed) {
        //was: doc.add(new Fieldable(fi.name, uncompress(b), Fieldable.Store.COMPRESS));
        doc.add(new LazyField(fi.name, Field.Store.COMPRESS, toRead, pointer));
      } else {
        //was: doc.add(new Fieldable(fi.name, b, Fieldable.Store.YES));
        doc.add(new LazyField(fi.name, Field.Store.YES, toRead, pointer));
      }
      //Need to move the pointer ahead by toRead positions
      fieldsStream.seek(pointer + toRead);
    } else {
      Field.Store store = Field.Store.YES;
      Field.Index index = getIndexType(fi, tokenize);
      Field.TermVector termVector = getTermVectorType(fi);

      Fieldable f;
      if (compressed) {
        store = Field.Store.COMPRESS;
        int toRead = fieldsStream.readVInt();
        long pointer = fieldsStream.getFilePointer();
        f = new LazyField(fi.name, store, toRead, pointer);
        //skip over the part that we aren't loading
        fieldsStream.seek(pointer + toRead);
        f.setOmitNorms(fi.omitNorms);
      } else {
        int length = fieldsStream.readVInt();
        long pointer = fieldsStream.getFilePointer();
        //Skip ahead of where we are by the length of what is stored
        fieldsStream.skipChars(length);
        f = new LazyField(fi.name, store, index, termVector, length, pointer);
        f.setOmitNorms(fi.omitNorms);
      }
      doc.add(f);
    }

  }

  // in merge mode we don't uncompress the data of a compressed field
  private void addFieldForMerge(Document doc, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize) throws IOException {
    Object data;
      
    if (binary || compressed) {
      int toRead = fieldsStream.readVInt();
      final byte[] b = new byte[toRead];
      fieldsStream.readBytes(b, 0, b.length);
      data = b;
    } else {
      data = fieldsStream.readString();
    }
      
    doc.add(new FieldForMerge(data, fi, binary, compressed, tokenize));
  }
  
  private void addField(Document doc, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize) throws CorruptIndexException, IOException {

    //we have a binary stored field, and it may be compressed
    if (binary) {
      int toRead = fieldsStream.readVInt();
      final byte[] b = new byte[toRead];
      fieldsStream.readBytes(b, 0, b.length);
      if (compressed)
        doc.add(new Field(fi.name, uncompress(b), Field.Store.COMPRESS));
      else
        doc.add(new Field(fi.name, b, Field.Store.YES));

    } else {
      Field.Store store = Field.Store.YES;
      Field.Index index = getIndexType(fi, tokenize);
      Field.TermVector termVector = getTermVectorType(fi);

      Fieldable f;
      if (compressed) {
        store = Field.Store.COMPRESS;
        int toRead = fieldsStream.readVInt();

        final byte[] b = new byte[toRead];
        fieldsStream.readBytes(b, 0, b.length);
        f = new Field(fi.name,      // field name
                new String(uncompress(b), "UTF-8"), // uncompress the value and add as string
                store,
                index,
                termVector);
        f.setOmitNorms(fi.omitNorms);
      } else {
        f = new Field(fi.name,     // name
                fieldsStream.readString(), // read value
                store,
                index,
                termVector);
        f.setOmitNorms(fi.omitNorms);
      }
      doc.add(f);
    }
  }
  
  // Add the size of field as a byte[] containing the 4 bytes of the integer byte size (high order byte first; char = 2 bytes)
  // Read just the size -- caller must skip the field content to continue reading fields
  // Return the size in bytes or chars, depending on field type
  private int addFieldSize(Document doc, FieldInfo fi, boolean binary, boolean compressed) throws IOException {
    int size = fieldsStream.readVInt(), bytesize = binary || compressed ? size : 2*size;
    byte[] sizebytes = new byte[4];
    sizebytes[0] = (byte) (bytesize>>>24);
    sizebytes[1] = (byte) (bytesize>>>16);
    sizebytes[2] = (byte) (bytesize>>> 8);
    sizebytes[3] = (byte)  bytesize      ;
    doc.add(new Field(fi.name, sizebytes, Field.Store.YES));
    return size;
  }

  private Field.TermVector getTermVectorType(FieldInfo fi) {
    Field.TermVector termVector = null;
    if (fi.storeTermVector) {
      if (fi.storeOffsetWithTermVector) {
        if (fi.storePositionWithTermVector) {
          termVector = Field.TermVector.WITH_POSITIONS_OFFSETS;
        } else {
          termVector = Field.TermVector.WITH_OFFSETS;
        }
      } else if (fi.storePositionWithTermVector) {
        termVector = Field.TermVector.WITH_POSITIONS;
      } else {
        termVector = Field.TermVector.YES;
      }
    } else {
      termVector = Field.TermVector.NO;
    }
    return termVector;
  }

  private Field.Index getIndexType(FieldInfo fi, boolean tokenize) {
    Field.Index index;
    if (fi.isIndexed && tokenize)
      index = Field.Index.TOKENIZED;
    else if (fi.isIndexed && !tokenize)
      index = Field.Index.UN_TOKENIZED;
    else
      index = Field.Index.NO;
    return index;
  }

  /**
   * A Lazy implementation of Fieldable that differs loading of fields until asked for, instead of when the Document is
   * loaded.
   */
  private class LazyField extends AbstractField implements Fieldable {
    private int toRead;
    private long pointer;

    public LazyField(String name, Field.Store store, int toRead, long pointer) {
      super(name, store, Field.Index.NO, Field.TermVector.NO);
      this.toRead = toRead;
      this.pointer = pointer;
      lazy = true;
    }

    public LazyField(String name, Field.Store store, Field.Index index, Field.TermVector termVector, int toRead, long pointer) {
      super(name, store, index, termVector);
      this.toRead = toRead;
      this.pointer = pointer;
      lazy = true;
    }

    private IndexInput getFieldStream() {
      IndexInput localFieldsStream = (IndexInput) fieldsStreamTL.get();
      if (localFieldsStream == null) {
        localFieldsStream = (IndexInput) cloneableFieldsStream.clone();
        fieldsStreamTL.set(localFieldsStream);
      }
      return localFieldsStream;
    }

    /**
     * The value of the field in Binary, or null.  If null, the Reader or
     * String value is used.  Exactly one of stringValue(), readerValue() and
     * binaryValue() must be set.
     */
    public byte[] binaryValue() {
      if (fieldsData == null) {
        final byte[] b = new byte[toRead];
        IndexInput localFieldsStream = getFieldStream();
        //Throw this IO Exception since IndexREader.document does so anyway, so probably not that big of a change for people
        //since they are already handling this exception when getting the document
        try {
          localFieldsStream.seek(pointer);
          localFieldsStream.readBytes(b, 0, b.length);
          if (isCompressed == true) {
            fieldsData = uncompress(b);
          } else {
            fieldsData = b;
          }
        } catch (IOException e) {
          throw new FieldReaderException(e);
        }
      }
      return fieldsData instanceof byte[] ? (byte[]) fieldsData : null;
    }

    /**
     * The value of the field as a Reader, or null.  If null, the String value
     * or binary value is  used.  Exactly one of stringValue(), readerValue(),
     * and binaryValue() must be set.
     */
    public Reader readerValue() {
      return fieldsData instanceof Reader ? (Reader) fieldsData : null;
    }

    /**
     * The value of the field as a String, or null.  If null, the Reader value
     * or binary value is used.  Exactly one of stringValue(), readerValue(), and
     * binaryValue() must be set.
     */
    public String stringValue() {
      if (fieldsData == null) {
        IndexInput localFieldsStream = getFieldStream();
        try {
          localFieldsStream.seek(pointer);
          if (isCompressed) {
            final byte[] b = new byte[toRead];
            localFieldsStream.readBytes(b, 0, b.length);
            fieldsData = new String(uncompress(b), "UTF-8");
          } else {
            //read in chars b/c we already know the length we need to read
            char[] chars = new char[toRead];
            localFieldsStream.readChars(chars, 0, toRead);
            fieldsData = new String(chars);
          }
        } catch (IOException e) {
          throw new FieldReaderException(e);
        }
      }
      return fieldsData instanceof String ? (String) fieldsData : null;
    }

    public long getPointer() {
      return pointer;
    }

    public void setPointer(long pointer) {
      this.pointer = pointer;
    }

    public int getToRead() {
      return toRead;
    }

    public void setToRead(int toRead) {
      this.toRead = toRead;
    }
  }

  private final byte[] uncompress(final byte[] input)
          throws CorruptIndexException, IOException {

    Inflater decompressor = new Inflater();
    decompressor.setInput(input);

    // Create an expandable byte array to hold the decompressed data
    ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);

    // Decompress the data
    byte[] buf = new byte[1024];
    while (!decompressor.finished()) {
      try {
        int count = decompressor.inflate(buf);
        bos.write(buf, 0, count);
      }
      catch (DataFormatException e) {
        // this will happen if the field is not compressed
        CorruptIndexException newException = new CorruptIndexException("field data are in wrong format: " + e.toString());
        newException.initCause(e);
        throw newException;
      }
    }
  
    decompressor.end();
    
    // Get the decompressed data
    return bos.toByteArray();
  }
  
  // Instances of this class hold field properties and data
  // for merge
  final static class FieldForMerge extends AbstractField {
    public String stringValue() {
      return (String) this.fieldsData;
    }

    public Reader readerValue() {
      // not needed for merge
      return null;
    }

    public byte[] binaryValue() {
      return (byte[]) this.fieldsData;
    }
    
    public FieldForMerge(Object value, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize) {
      this.isStored = true;  
      this.fieldsData = value;
      this.isCompressed = compressed;
      this.isBinary = binary;
      this.isTokenized = tokenize;

      this.name = fi.name.intern();
      this.isIndexed = fi.isIndexed;
      this.omitNorms = fi.omitNorms;          
      this.storeOffsetWithTermVector = fi.storeOffsetWithTermVector;
      this.storePositionWithTermVector = fi.storePositionWithTermVector;
      this.storeTermVector = fi.storeTermVector;            
    }
     
  }
}
