package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.Deflater;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;

final class FieldsWriter
{
  static final byte FIELD_IS_TOKENIZED = 0x1;
  static final byte FIELD_IS_BINARY = 0x2;
  static final byte FIELD_IS_COMPRESSED = 0x4;
  
    private FieldInfos fieldInfos;

    private IndexOutput fieldsStream;

    private IndexOutput indexStream;

    private boolean doClose;

    FieldsWriter(Directory d, String segment, FieldInfos fn) throws IOException {
        fieldInfos = fn;
        fieldsStream = d.createOutput(segment + ".fdt");
        indexStream = d.createOutput(segment + ".fdx");
        doClose = true;
    }

    FieldsWriter(IndexOutput fdx, IndexOutput fdt, FieldInfos fn) throws IOException {
        fieldInfos = fn;
        fieldsStream = fdt;
        indexStream = fdx;
        doClose = false;
    }

    // Writes the contents of buffer into the fields stream
    // and adds a new entry for this document into the index
    // stream.  This assumes the buffer was already written
    // in the correct fields format.
    void flushDocument(RAMOutputStream buffer) throws IOException {
      indexStream.writeLong(fieldsStream.getFilePointer());
      buffer.writeTo(fieldsStream);
    }

    void flush() throws IOException {
      indexStream.flush();
      fieldsStream.flush();
    }

    final void close() throws IOException {
      if (doClose) {
        fieldsStream.close();
        indexStream.close();
      }
    }

    final void writeField(FieldInfo fi, Fieldable field) throws IOException {
      // if the field as an instanceof FieldsReader.FieldForMerge, we're in merge mode
      // and field.binaryValue() already returns the compressed value for a field
      // with isCompressed()==true, so we disable compression in that case
      boolean disableCompression = (field instanceof FieldsReader.FieldForMerge);
      fieldsStream.writeVInt(fi.number);
      byte bits = 0;
      if (field.isTokenized())
        bits |= FieldsWriter.FIELD_IS_TOKENIZED;
      if (field.isBinary())
        bits |= FieldsWriter.FIELD_IS_BINARY;
      if (field.isCompressed())
        bits |= FieldsWriter.FIELD_IS_COMPRESSED;
                
      fieldsStream.writeByte(bits);
                
      if (field.isCompressed()) {
        // compression is enabled for the current field
        byte[] data = null;
                  
        if (disableCompression) {
          // optimized case for merging, the data
          // is already compressed
          data = field.binaryValue();
        } else {
          // check if it is a binary field
          if (field.isBinary()) {
            data = compress(field.binaryValue());
          }
          else {
            data = compress(field.stringValue().getBytes("UTF-8"));
          }
        }
        final int len = data.length;
        fieldsStream.writeVInt(len);
        fieldsStream.writeBytes(data, len);
      }
      else {
        // compression is disabled for the current field
        if (field.isBinary()) {
          byte[] data = field.binaryValue();
          final int len = data.length;
          fieldsStream.writeVInt(len);
          fieldsStream.writeBytes(data, len);
        }
        else {
          fieldsStream.writeString(field.stringValue());
        }
      }
    }

    /** Bulk write a contiguous series of documents.  The
     *  lengths array is the length (in bytes) of each raw
     *  document.  The stream IndexInput is the
     *  fieldsStream from which we should bulk-copy all
     *  bytes. */
    final void addRawDocuments(IndexInput stream, int[] lengths, int numDocs) throws IOException {
      long position = fieldsStream.getFilePointer();
      long start = position;
      for(int i=0;i<numDocs;i++) {
        indexStream.writeLong(position);
        position += lengths[i];
      }
      fieldsStream.copyBytes(stream, position-start);
    }

    final void addDocument(Document doc) throws IOException {
        indexStream.writeLong(fieldsStream.getFilePointer());

        int storedCount = 0;
        Iterator fieldIterator = doc.getFields().iterator();
        while (fieldIterator.hasNext()) {
            Fieldable field = (Fieldable) fieldIterator.next();
            if (field.isStored())
                storedCount++;
        }
        fieldsStream.writeVInt(storedCount);

        fieldIterator = doc.getFields().iterator();
        while (fieldIterator.hasNext()) {
            Fieldable field = (Fieldable) fieldIterator.next();
            if (field.isStored())
              writeField(fieldInfos.fieldInfo(field.name()), field);
        }
    }

    private final byte[] compress (byte[] input) {

      // Create the compressor with highest level of compression
      Deflater compressor = new Deflater();
      compressor.setLevel(Deflater.BEST_COMPRESSION);

      // Give the compressor the data to compress
      compressor.setInput(input);
      compressor.finish();

      /*
       * Create an expandable byte array to hold the compressed data.
       * You cannot use an array that's the same size as the orginal because
       * there is no guarantee that the compressed data will be smaller than
       * the uncompressed data.
       */
      ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);

      // Compress the data
      byte[] buf = new byte[1024];
      while (!compressor.finished()) {
        int count = compressor.deflate(buf);
        bos.write(buf, 0, count);
      }
      
      compressor.end();

      // Get the compressed data
      return bos.toByteArray();
    }
}
