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

import java.util.Enumeration;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.OutputStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

final class FieldsWriter {
  private FieldInfos fieldInfos;
  private OutputStream fieldsStream;
  private OutputStream indexStream;
  
  FieldsWriter(Directory d, String segment, FieldInfos fn)
       throws IOException {
    fieldInfos = fn;
    fieldsStream = d.createFile(segment + ".fdt");
    indexStream = d.createFile(segment + ".fdx");
  }

  final void close() throws IOException {
    fieldsStream.close();
    indexStream.close();
  }

  final void addDocument(Document doc) throws IOException {
    indexStream.writeLong(fieldsStream.getFilePointer());
    
    int storedCount = 0;
    Enumeration fields  = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field)fields.nextElement();
      if (field.isStored())
	storedCount++;
    }
    fieldsStream.writeVInt(storedCount);
    
    fields  = doc.fields();
    while (fields.hasMoreElements()) {
      Field field = (Field)fields.nextElement();
      if (field.isStored()) {
	fieldsStream.writeVInt(fieldInfos.fieldNumber(field.name()));

	byte bits = 0;
	if (field.isTokenized())
	  bits |= 1;
	fieldsStream.writeByte(bits);

	fieldsStream.writeString(field.stringValue());
      }
    }
  }
}
