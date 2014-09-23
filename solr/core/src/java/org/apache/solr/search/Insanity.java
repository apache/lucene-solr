package org.apache.solr.search;

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
import java.util.Collections;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.uninverting.UninvertingReader;

/** 
 * Lucene 5.0 removes "accidental" insanity, so you must explicitly
 * create it.
 * <p>
 * This class creates insanity for two specific situations:
 * <ul>
 *   <li>calling {@code ord} or {@code rord} functions on a single-valued numeric field.
 *   <li>doing grouped faceting ({@code group.facet}) on a single-valued numeric field.
 * </ul>
 */
@Deprecated
public class Insanity {
  
  /** 
   * Returns a view over {@code sane} where {@code insaneField} is a string
   * instead of a numeric.
   */
  public static LeafReader wrapInsanity(LeafReader sane, String insaneField) {
    return new UninvertingReader(new InsaneReader(sane, insaneField),
                                 Collections.singletonMap(insaneField, UninvertingReader.Type.SORTED));
  }
  
  /** Hides the proper numeric dv type for the field */
  private static class InsaneReader extends FilterLeafReader {
    final String insaneField;
    final FieldInfos fieldInfos;
    
    InsaneReader(LeafReader in, String insaneField) {
      super(in);
      this.insaneField = insaneField;
      ArrayList<FieldInfo> filteredInfos = new ArrayList<>();
      for (FieldInfo fi : in.getFieldInfos()) {
        if (fi.name.equals(insaneField)) {
          filteredInfos.add(new FieldInfo(fi.name, fi.isIndexed(), fi.number, fi.hasVectors(), fi.omitsNorms(),
                                          fi.hasPayloads(), fi.getIndexOptions(), null, fi.getNormType(), -1, null));
        } else {
          filteredInfos.add(fi);
        }
      }
      fieldInfos = new FieldInfos(filteredInfos.toArray(new FieldInfo[filteredInfos.size()]));
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
      if (insaneField.equals(field)) {
        return null;
      } else {
        return in.getNumericDocValues(field);
      }
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
      if (insaneField.equals(field)) {
        return null;
      } else {
        return in.getBinaryDocValues(field);
      }
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
      if (insaneField.equals(field)) {
        return null;
      } else {
        return in.getSortedDocValues(field);
      }
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
      if (insaneField.equals(field)) {
        return null;
      } else {
        return in.getSortedSetDocValues(field);
      }
    }

    @Override
    public FieldInfos getFieldInfos() {
      return fieldInfos;
    }

    // important to override these, so fieldcaches are shared on what we wrap
    
    @Override
    public Object getCoreCacheKey() {
      return in.getCoreCacheKey();
    }

    @Override
    public Object getCombinedCoreAndDeletesKey() {
      return in.getCombinedCoreAndDeletesKey();
    }
  }
}
