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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;

/**
 * Class responsible for access to stored document fields.
 *
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 *
 * @version $Id$
 */
final class FieldsReader {
  private FieldInfos fieldInfos;
  private IndexInput fieldsStream;
  private IndexInput indexStream;
  private int size;

  FieldsReader(Directory d, String segment, FieldInfos fn) throws IOException {
    fieldInfos = fn;

    fieldsStream = d.openInput(segment + ".fdt");
    indexStream = d.openInput(segment + ".fdx");

    size = (int)(indexStream.length() / 8);
  }

  final void close() throws IOException {
    fieldsStream.close();
    indexStream.close();
  }

  final int size() {
    return size;
  }

  final Document doc(int n) throws IOException {
    indexStream.seek(n * 8L);
    long position = indexStream.readLong();
    fieldsStream.seek(position);

    Document doc = new Document();
    int numFields = fieldsStream.readVInt();
    for (int i = 0; i < numFields; i++) {
      int fieldNumber = fieldsStream.readVInt();
      FieldInfo fi = fieldInfos.fieldInfo(fieldNumber);

      byte bits = fieldsStream.readByte();
      
      boolean compressed = (bits & FieldsWriter.FIELD_IS_COMPRESSED) != 0;
      boolean tokenize = (bits & FieldsWriter.FIELD_IS_TOKENIZED) != 0;
      
      if ((bits & FieldsWriter.FIELD_IS_BINARY) != 0) {
        final byte[] b = new byte[fieldsStream.readVInt()];
        fieldsStream.readBytes(b, 0, b.length);
        if (compressed)
          doc.add(new Field(fi.name, uncompress(b), Field.Store.COMPRESS));
        else
          doc.add(new Field(fi.name, b, Field.Store.YES));
      }
      else {
        Field.Index index;
        Field.Store store = Field.Store.YES;
        
        if (fi.isIndexed && tokenize)
          index = Field.Index.TOKENIZED;
        else if (fi.isIndexed && !tokenize)
          index = Field.Index.UN_TOKENIZED;
        else
          index = Field.Index.NO;
        
        if (compressed) {
          store = Field.Store.COMPRESS;
          final byte[] b = new byte[fieldsStream.readVInt()];
          fieldsStream.readBytes(b, 0, b.length);
          doc.add(new Field(fi.name,      // field name
              new String(uncompress(b), "UTF-8"), // uncompress the value and add as string
              store,
              index,
              fi.storeTermVector ? Field.TermVector.YES : Field.TermVector.NO));
        }
        else
          doc.add(new Field(fi.name,      // name
                fieldsStream.readString(), // read value
                store,
                index,
                fi.storeTermVector ? Field.TermVector.YES : Field.TermVector.NO));
      }
    }

    return doc;
  }
  
  private final byte[] uncompress(final byte[] input)
    throws IOException
  {
  
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
        throw new IOException ("field data are in wrong format: " + e.toString());
      }
    }
  
    decompressor.end();
    
    // Get the decompressed data
    return bos.toByteArray();
  }
}
