package org.apache.lucene.uninverting;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;

public class UninvertingReader extends FilterAtomicReader {
  
  public static enum Type {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BINARY,
    SORTED,
    SORTED_SET
  }
  
  public static DirectoryReader wrap(DirectoryReader in, final Map<String,Type> mapping) {
    return new UninvertingDirectoryReader(in, mapping);
  }
  
  static class UninvertingDirectoryReader extends FilterDirectoryReader {
    final Map<String,Type> mapping;
    
    public UninvertingDirectoryReader(DirectoryReader in, final Map<String,Type> mapping) {
      super(in, new FilterDirectoryReader.SubReaderWrapper() {
        @Override
        public AtomicReader wrap(AtomicReader reader) {
          return new UninvertingReader(reader, mapping);
        }
      });
      this.mapping = mapping;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
      return new UninvertingDirectoryReader(in, mapping);
    }
  }
  
  final Map<String,Type> mapping;
  final FieldInfos fieldInfos;
  
  UninvertingReader(AtomicReader in, Map<String,Type> mapping) {
    super(in);
    this.mapping = mapping;
    ArrayList<FieldInfo> filteredInfos = new ArrayList<>();
    for (FieldInfo fi : in.getFieldInfos()) {
      FieldInfo.DocValuesType type = fi.getDocValuesType();
      if (fi.isIndexed() && !fi.hasDocValues()) {
        Type t = mapping.get(fi.name);
        if (t != null) {
          switch(t) {
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
              type = FieldInfo.DocValuesType.NUMERIC;
              break;
            case BINARY:
              type = FieldInfo.DocValuesType.BINARY;
              break;
            case SORTED:
              type = FieldInfo.DocValuesType.SORTED;
              break;
            case SORTED_SET:
              type = FieldInfo.DocValuesType.SORTED_SET;
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      filteredInfos.add(new FieldInfo(fi.name, fi.isIndexed(), fi.number, fi.hasVectors(), fi.omitsNorms(),
                                      fi.hasPayloads(), fi.getIndexOptions(), type, fi.getNormType(), null));
    }
    fieldInfos = new FieldInfos(filteredInfos.toArray(new FieldInfo[filteredInfos.size()]));
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    Type v = mapping.get(field);
    if (v != null) {
      switch (mapping.get(field)) {
        case INTEGER: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_INT_PARSER, true);
        case FLOAT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_FLOAT_PARSER, true);
        case LONG: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_LONG_PARSER, true);
        case DOUBLE: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, true);
      }
    }
    return super.getNumericDocValues(field);
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    if (mapping.get(field) == Type.BINARY) {
      return FieldCache.DEFAULT.getTerms(in, field, true);
    } else {
      return in.getBinaryDocValues(field);
    }
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    if (mapping.get(field) == Type.SORTED) {
      return FieldCache.DEFAULT.getTermsIndex(in, field);
    } else {
      return in.getSortedDocValues(field);
    }
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    if (mapping.get(field) == Type.SORTED_SET) {
      return FieldCache.DEFAULT.getDocTermOrds(in, field);
    } else {
      return in.getSortedSetDocValues(field);
    }
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    if (mapping.containsKey(field)) {
      return FieldCache.DEFAULT.getDocsWithField(in, field);
    } else {
      return in.getDocsWithField(field);
    }
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }

  @Override
  public String toString() {
    return "Uninverting(" + in.toString() + ")";
  }
}
