package org.apache.lucene.document;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.NumericField.DataType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldReaderException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/** Create this, passing a legacy {@link FieldSelector} to it, then
 *  pass this class to {@link IndexReader#document(int,
 *  StoredFieldVisitor)}, then call {@link #getDocument} to
 *  retrieve the loaded document.

 *  <p><b>NOTE</b>:  If you use Lazy fields, you should not
 *  access the returned document after the reader has been
 *  closed!
 */

public class FieldSelectorVisitor extends StoredFieldVisitor {

  private final FieldSelector selector;
  private final Document doc;

  public FieldSelectorVisitor(FieldSelector selector) {
    this.selector = selector;
    doc = new Document();
  }

  public Document getDocument() {
    return doc;
  }

  @Override
  public boolean binaryField(FieldInfo fieldInfo, IndexInput in, int numBytes) throws IOException {
    final FieldSelectorResult accept = selector.accept(fieldInfo.name);
    switch (accept) {
    case LOAD:
    case LOAD_AND_BREAK:
      final byte[] b = new byte[numBytes];
      in.readBytes(b, 0, b.length);
      doc.add(new BinaryField(fieldInfo.name, b));
      return accept != FieldSelectorResult.LOAD;
    case LAZY_LOAD:
    case LATENT:
      addFieldLazy(in, fieldInfo, true, accept == FieldSelectorResult.LAZY_LOAD, numBytes);
      return false;
    case SIZE:
    case SIZE_AND_BREAK:
      in.seek(in.getFilePointer() + numBytes);
      addFieldSize(fieldInfo, numBytes);
      return accept != FieldSelectorResult.SIZE;
    default:
      // skip
      in.seek(in.getFilePointer() + numBytes);
      return false;
    }
  }

  @Override
  public boolean stringField(FieldInfo fieldInfo, IndexInput in, int numUTF8Bytes) throws IOException {
    final FieldSelectorResult accept = selector.accept(fieldInfo.name);
    switch (accept) {
    case LOAD:
    case LOAD_AND_BREAK:
      final byte[] b = new byte[numUTF8Bytes];
      in.readBytes(b, 0, b.length);
      FieldType ft = new FieldType(TextField.TYPE_STORED);
      ft.setStoreTermVectors(fieldInfo.storeTermVector);
      ft.setStoreTermVectorOffsets(fieldInfo.storeOffsetWithTermVector);
      ft.setStoreTermVectorPositions(fieldInfo.storePositionWithTermVector);
      doc.add(new Field(fieldInfo.name, ft, new String(b, "UTF-8"))); 
      return accept != FieldSelectorResult.LOAD;
    case LAZY_LOAD:
    case LATENT:
      addFieldLazy(in, fieldInfo, false, accept == FieldSelectorResult.LAZY_LOAD, numUTF8Bytes);
      return false;
    case SIZE:
    case SIZE_AND_BREAK:
      in.seek(in.getFilePointer() + numUTF8Bytes);
      addFieldSize(fieldInfo, 2*numUTF8Bytes);
      return accept != FieldSelectorResult.SIZE;
    default:
      // skip
      in.seek(in.getFilePointer() + numUTF8Bytes);
      return false;
    }
  }

  @Override
  public boolean intField(FieldInfo fieldInfo, int value) throws IOException {
		FieldType ft = new FieldType(NumericField.TYPE_STORED);
		ft.setIndexed(fieldInfo.isIndexed);
		ft.setOmitNorms(fieldInfo.omitNorms);
		ft.setIndexOptions(fieldInfo.indexOptions);
    return addNumericField(fieldInfo, new NumericField(fieldInfo.name, ft).setIntValue(value));
  }

  @Override
  public boolean longField(FieldInfo fieldInfo, long value) throws IOException { 
		FieldType ft = new FieldType(NumericField.TYPE_STORED);
		ft.setIndexed(fieldInfo.isIndexed);
		ft.setOmitNorms(fieldInfo.omitNorms);
		ft.setIndexOptions(fieldInfo.indexOptions);
    return addNumericField(fieldInfo, new NumericField(fieldInfo.name, ft).setLongValue(value));
  }

  @Override
  public boolean floatField(FieldInfo fieldInfo, float value) throws IOException {
		FieldType ft = new FieldType(NumericField.TYPE_STORED);
		ft.setIndexed(fieldInfo.isIndexed);
		ft.setOmitNorms(fieldInfo.omitNorms);
		ft.setIndexOptions(fieldInfo.indexOptions);
    return addNumericField(fieldInfo, new NumericField(fieldInfo.name, ft).setFloatValue(value));
  }

  @Override
  public boolean doubleField(FieldInfo fieldInfo, double value) throws IOException {
		FieldType ft = new FieldType(NumericField.TYPE_STORED);
		ft.setIndexed(fieldInfo.isIndexed);
		ft.setOmitNorms(fieldInfo.omitNorms);
		ft.setIndexOptions(fieldInfo.indexOptions);
    return addNumericField(fieldInfo, new NumericField(fieldInfo.name, ft).setDoubleValue(value));
  }

  private boolean addNumericField(FieldInfo fieldInfo, NumericField f) {
    doc.add(f);
    final FieldSelectorResult accept = selector.accept(fieldInfo.name);
    switch (accept) {
    case LOAD:
      return false;
    case LOAD_AND_BREAK:
      return true;
    case LAZY_LOAD:
    case LATENT:
      return false;
    case SIZE:
      return false;
    case SIZE_AND_BREAK:
      return true;
    default:
      return false;
    }
  }

  private void addFieldLazy(IndexInput in, FieldInfo fi, boolean binary, boolean cacheResult, int numBytes) throws IOException {
    final IndexableField f;
    final long pointer = in.getFilePointer();
    // Need to move the pointer ahead by toRead positions
    in.seek(pointer+numBytes);
    FieldType ft = new FieldType();
    ft.setStored(true);
    ft.setOmitNorms(fi.omitNorms);
    ft.setIndexOptions(fi.indexOptions);
    
    if (binary) {
      f = new LazyField(in, fi.name, ft, numBytes, pointer, binary, cacheResult);
    } else {
      ft.setStoreTermVectors(fi.storeTermVector);
      ft.setStoreTermVectorOffsets(fi.storeOffsetWithTermVector);
      ft.setStoreTermVectorPositions(fi.storePositionWithTermVector);
      f = new LazyField(in, fi.name, ft, numBytes, pointer, binary, cacheResult);
    }
    
    doc.add(f);
  }

  // Add the size of field as a byte[] containing the 4 bytes of the integer byte size (high order byte first; char = 2 bytes)
  // Read just the size -- caller must skip the field content to continue reading fields
  // Return the size in bytes or chars, depending on field type
  private void addFieldSize(FieldInfo fi, int numBytes) throws IOException {
    byte[] sizebytes = new byte[4];
    sizebytes[0] = (byte) (numBytes>>>24);
    sizebytes[1] = (byte) (numBytes>>>16);
    sizebytes[2] = (byte) (numBytes>>> 8);
    sizebytes[3] = (byte)  numBytes      ;
    doc.add(new BinaryField(fi.name, sizebytes));
  }

  /**
   * A Lazy field implementation that defers loading of fields until asked for, instead of when the Document is
   * loaded.
   */
  private static class LazyField extends Field {
    private int toRead;
    private long pointer;
    private final boolean cacheResult;
    private final IndexInput in;
    private boolean isBinary;

    public LazyField(IndexInput in, String name, FieldType ft, int toRead, long pointer, boolean isBinary, boolean cacheResult) {
      super(name, ft);
      this.in = in;
      this.toRead = toRead;
      this.pointer = pointer;
      this.isBinary = isBinary;
      this.cacheResult = cacheResult;
    }

    @Override
    public Number numericValue() {
      return null;
    }

    @Override
    public DataType numericDataType() {
      return null;
    }

    private IndexInput localFieldsStream;

    private IndexInput getFieldStream() {
      if (localFieldsStream == null) {
        localFieldsStream = (IndexInput) in.clone();
      }
      return localFieldsStream;
    }

    /** The value of the field as a Reader, or null.  If null, the String value,
     * binary value, or TokenStream value is used.  Exactly one of stringValue(), 
     * readerValue(), getBinaryValue(), and tokenStreamValue() must be set. */
    @Override
    public Reader readerValue() {
      return null;
    }

    /** The value of the field as a TokenStream, or null.  If null, the Reader value,
     * String value, or binary value is used. Exactly one of stringValue(), 
     * readerValue(), getBinaryValue(), and tokenStreamValue() must be set. */
    @Override
    public TokenStream tokenStreamValue() {
      return null;
    }

    /** The value of the field as a String, or null.  If null, the Reader value,
     * binary value, or TokenStream value is used.  Exactly one of stringValue(), 
     * readerValue(), getBinaryValue(), and tokenStreamValue() must be set. */
    @Override
    synchronized public String stringValue() {
      if (isBinary) {
        return null;
      } else {
        if (fieldsData == null) {
          String result = null;
          IndexInput localFieldsStream = getFieldStream();
          try {
            localFieldsStream.seek(pointer);
            byte[] bytes = new byte[toRead];
            localFieldsStream.readBytes(bytes, 0, toRead);
            result = new String(bytes, "UTF-8");
          } catch (IOException e) {
            throw new FieldReaderException(e);
          }
          if (cacheResult == true){
            fieldsData = result;
          }
          return result;
        } else {
          return (String) fieldsData;
        }
      }
    }

    @Override
    synchronized public BytesRef binaryValue() {
      if (isBinary) {
        if (fieldsData == null) {
          // Allocate new buffer if result is null or too small
          final byte[] b = new byte[toRead];
   
          IndexInput localFieldsStream = getFieldStream();

          // Throw this IOException since IndexReader.document does so anyway, so probably not that big of a change for people
          // since they are already handling this exception when getting the document
          try {
            localFieldsStream.seek(pointer);
            localFieldsStream.readBytes(b, 0, toRead);
          } catch (IOException e) {
            throw new FieldReaderException(e);
          }

          final BytesRef result = new BytesRef(b);
          result.length = toRead;
          if (cacheResult == true){
            fieldsData = result;
          }
          return result;
        } else {
          return (BytesRef) fieldsData;
        }
      } else {
        return null;
      }
    }
  }
}
