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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.InputStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * Class responsible for access to stored document fields.
 *
 * It uses &lt;segment&gt;.fdt and &lt;segment&gt;.fdx; files.
 *
 * @version $Id$
 */
final class FieldsReader {
  private FieldInfos fieldInfos;
  private InputStream fieldsStream;
  private InputStream indexStream;
  private int size;

  FieldsReader(Directory d, String segment, FieldInfos fn) throws IOException {
    fieldInfos = fn;

    fieldsStream = d.openFile(segment + ".fdt");
    indexStream = d.openFile(segment + ".fdx");

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

      Field.Index index;
      boolean tokenize = (bits & 1) != 0;
      if (fi.isIndexed && tokenize)
        index = Field.Index.TOKENIZED;
      else if (fi.isIndexed && !tokenize)
        index = Field.Index.UN_TOKENIZED;
      else
        index = Field.Index.NO;
      doc.add(new Field(fi.name,		  // name
			fieldsStream.readString(), // read value
			Field.Store.YES, index,
			fi.storeTermVector ? Field.TermVector.YES : Field.TermVector.NO));
    }

    return doc;
  }
}
