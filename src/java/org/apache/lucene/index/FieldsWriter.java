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
import java.util.Enumeration;
import java.util.zip.Deflater;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;

final class FieldsWriter
{
  static final byte FIELD_IS_TOKENIZED = 0x1;
  static final byte FIELD_IS_BINARY = 0x2;
  static final byte FIELD_IS_COMPRESSED = 0x4;
  
    private FieldInfos fieldInfos;

    private IndexOutput fieldsStream;

    private IndexOutput indexStream;

    FieldsWriter(Directory d, String segment, FieldInfos fn) throws IOException {
        fieldInfos = fn;
        fieldsStream = d.createOutput(segment + ".fdt");
        indexStream = d.createOutput(segment + ".fdx");
    }

    final void close() throws IOException {
        fieldsStream.close();
        indexStream.close();
    }

    final void addDocument(Document doc) throws IOException {
        indexStream.writeLong(fieldsStream.getFilePointer());

        int storedCount = 0;
        Enumeration fields = doc.fields();
        while (fields.hasMoreElements()) {
            Field field = (Field) fields.nextElement();
            if (field.isStored())
                storedCount++;
        }
        fieldsStream.writeVInt(storedCount);

        fields = doc.fields();
        while (fields.hasMoreElements()) {
            Field field = (Field) fields.nextElement();
            if (field.isStored()) {
                fieldsStream.writeVInt(fieldInfos.fieldNumber(field.name()));

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
                  // check if it is a binary field
                  if (field.isBinary()) {
                    data = compress(field.binaryValue());
                  }
                  else {
                    data = compress(field.stringValue().getBytes("UTF-8"));
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
