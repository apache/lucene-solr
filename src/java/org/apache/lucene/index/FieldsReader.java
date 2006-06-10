package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
  private FieldInfos fieldInfos;
  private IndexInput fieldsStream;
  private IndexInput indexStream;
  private int size;

  private static ThreadLocal fieldsStreamTL = new ThreadLocal();

  FieldsReader(Directory d, String segment, FieldInfos fn) throws IOException {
    fieldInfos = fn;

    fieldsStream = d.openInput(segment + ".fdt");
    indexStream = d.openInput(segment + ".fdx");
    size = (int) (indexStream.length() / 8);
  }

  /**
   * Cloeses the underlying {@link org.apache.lucene.store.IndexInput} streams, including any ones associated with a
   * lazy implementation of a Field.  This means that the Fields values will not be accessible.
   *
   * @throws IOException
   */
  final void close() throws IOException {
    fieldsStream.close();
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

  final Document doc(int n, FieldSelector fieldSelector) throws IOException {
    indexStream.seek(n * 8L);
    long position = indexStream.readLong();
    fieldsStream.seek(position);

    Document doc = new Document();
    int numFields = fieldsStream.readVInt();
    for (int i = 0; i < numFields; i++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fi = fieldInfos.fieldInfo(fieldNumber);
      FieldSelectorResult acceptField = fieldSelector == null ? FieldSelectorResult.LOAD : fieldSelector.accept(fi.name);
      boolean lazy = acceptField.equals(FieldSelectorResult.LAZY_LOAD) == true;
      
      byte bits = fieldsStream.readByte();
      boolean compressed = (bits & FieldsWriter.FIELD_IS_COMPRESSED) != 0;
      boolean tokenize = (bits & FieldsWriter.FIELD_IS_TOKENIZED) != 0;
      boolean binary = (bits & FieldsWriter.FIELD_IS_BINARY) != 0;
      if (acceptField.equals(FieldSelectorResult.LOAD) == true) {
        addField(doc, fi, binary, compressed, tokenize);
      }
      else if (acceptField.equals(FieldSelectorResult.LOAD_AND_BREAK) == true){
        addField(doc, fi, binary, compressed, tokenize);
        break;//Get out of this loop
      }
      else if (lazy == true){
        addFieldLazy(doc, fi, binary, compressed, tokenize);
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

    int toRead = fieldsStream.readVInt();

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

  private void addField(Document doc, FieldInfo fi, boolean binary, boolean compressed, boolean tokenize) throws IOException {

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
    //internal buffer
    private char[] chars;


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

    /**
     * The value of the field in Binary, or null.  If null, the Reader or
     * String value is used.  Exactly one of stringValue(), readerValue() and
     * binaryValue() must be set.
     */
    public byte[] binaryValue() {
      if (fieldsData == null) {
        final byte[] b = new byte[toRead];
        IndexInput localFieldsStream = (IndexInput) fieldsStreamTL.get();
        if (localFieldsStream == null) {
          localFieldsStream = (IndexInput) fieldsStream.clone();
          fieldsStreamTL.set(localFieldsStream);
        }
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
        IndexInput localFieldsStream = (IndexInput) fieldsStreamTL.get();
        if (localFieldsStream == null) {
          localFieldsStream = (IndexInput) fieldsStream.clone();
          fieldsStreamTL.set(localFieldsStream);
        }
        try {
          localFieldsStream.seek(pointer);
          //read in chars b/c we already know the length we need to read
          if (chars == null || toRead > chars.length)
            chars = new char[toRead];
          localFieldsStream.readChars(chars, 0, toRead);
          fieldsData = new String(chars, 0, toRead);//fieldsStream.readString();
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
          throws IOException {

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
        IOException newException = new IOException("field data are in wrong format: " + e.toString());
        newException.initCause(e);
        throw newException;
      }
    }
  
    decompressor.end();
    
    // Get the decompressed data
    return bos.toByteArray();
  }
}
