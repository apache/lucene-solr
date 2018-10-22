/*
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

package org.apache.solr.client.solrj.io.stream;


import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.JavaBinCodec;

public class JavabinTupleStreamParser extends JavaBinCodec implements TupleStreamParser {
  private final InputStream is;
  final FastInputStream fis;
  private int arraySize = Integer.MAX_VALUE;
  private boolean onlyJsonTypes = false;
  int objectSize;


  public JavabinTupleStreamParser(InputStream is, boolean onlyJsonTypes) throws IOException {
    this.onlyJsonTypes = onlyJsonTypes;
    this.is = is;
    this.fis = initRead(is);
    if (!readTillDocs()) arraySize = 0;
  }


  private boolean readTillDocs() throws IOException {
    if (isObjectType(fis)) {
      if (tagByte == SOLRDOCLST) {
        readVal(fis);// this is the metadata, throw it away
        tagByte = fis.readByte();
        arraySize = readSize(fis);
        return true;
      }
      for (int i = objectSize; i > 0; i--) {
        Object k = readVal(fis);
        if (k == END_OBJ) break;
        if ("docs".equals(k)) {
          tagByte = fis.readByte();
          if (tagByte == ITERATOR) return true;//docs must be an iterator or
          if (tagByte >>> 5 == ARR >>> 5) {// an array
            arraySize = readSize(fis);
            return true;
          }
          return false;
        } else {
          if (readTillDocs()) return true;
        }
      }
    } else {
      readObject(fis);
      return false;
    }
    return false;

    //here after it will be a stream of maps
  }

  private boolean isObjectType(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    if (tagByte >>> 5 == ORDERED_MAP >>> 5 ||
        tagByte >>> 5 == NAMED_LST >>> 5) {
      objectSize = readSize(dis);
      return true;
    }
    if (tagByte == MAP) {
      objectSize = readVInt(dis);
      return true;
    }
    if (tagByte == MAP_ENTRY_ITER) {
      objectSize = Integer.MAX_VALUE;
      return true;
    }
    return tagByte == SOLRDOCLST;
  }

  private Map readAsMap(DataInputInputStream dis) throws IOException {
    int sz = readSize(dis);
    Map m = new LinkedHashMap<>();
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(dis);
      Object val = readVal(dis);
      m.put(name, val);
    }
    return m;
  }

  private Map readSolrDocumentAsMap(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    int size = readSize(dis);
    Map doc = new LinkedHashMap<>();
    for (int i = 0; i < size; i++) {
      String fieldName;
      Object obj = readVal(dis); // could be a field name, or a child document
      if (obj instanceof Map) {
        List l = (List) doc.get("_childDocuments_");
        if (l == null) doc.put("_childDocuments_", l = new ArrayList());
        l.add(obj);
        continue;
      } else {
        fieldName = (String) obj;
      }
      Object fieldVal = readVal(dis);
      doc.put(fieldName, fieldVal);
    }
    return doc;
  }

  @Override
  protected Object readObject(DataInputInputStream dis) throws IOException {
    if (tagByte == SOLRDOC) {
      return readSolrDocumentAsMap(dis);
    }
    if (onlyJsonTypes) {
      switch (tagByte >>> 5) {
        case SINT >>> 5:
          int i = readSmallInt(dis);
          return (long) i;
        case ORDERED_MAP >>> 5:
          return readAsMap(dis);
        case NAMED_LST >>> 5:
          return readAsMap(dis);
      }

      switch (tagByte) {
        case INT: {
          int i = dis.readInt();
          return (long) i;
        }
        case FLOAT: {
          float v = dis.readFloat();
          return (double) v;
        }
        case BYTE: {
          byte b = dis.readByte();
          return (long) b;
        }
        case SHORT: {
          short s = dis.readShort();
          return (long) s;
        }

        case DATE: {
          return Instant.ofEpochMilli(dis.readLong()).toString();
        }

        default:
          return super.readObject(dis);
      }
    } else return super.readObject(dis);
  }


  @Override
  public Map<String, Object> next() throws IOException {
    if (arraySize == 0) return null;
    Object o = readVal(fis);
    arraySize--;
    if (o == END_OBJ) return null;
    return (Map<String, Object>) o;
  }

  @Override
  public void close() throws IOException {
    is.close();
  }
}
