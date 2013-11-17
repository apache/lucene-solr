package org.apache.lucene.facet.simple;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexDocument;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

public class FacetIndexWriter extends IndexWriter {

  private final TaxonomyWriter taxoWriter;
  private final FacetsConfig facetsConfig;

  public FacetIndexWriter(Directory d, IndexWriterConfig conf, TaxonomyWriter taxoWriter, FacetsConfig facetsConfig) throws IOException {
    super(d, conf);
    this.taxoWriter = taxoWriter;
    this.facetsConfig = facetsConfig;
  }

  // nocommit maybe we could somehow "own" TaxonomyWriter
  // too?  commit it in commit, close it in close, etc?

  // nocommit also updateDocument, addDocument, addDocuments

  @Override
  public void addDocument(final IndexDocument doc) throws IOException {

    // Find all FacetFields, collated by the actual field:
    Map<String,List<FacetField>> byField = new HashMap<String,List<FacetField>>();

    // ... and also all SortedSetDocValuesFacetFields:
    Map<String,List<SortedSetDocValuesFacetField>> dvByField = new HashMap<String,List<SortedSetDocValuesFacetField>>();

    for(IndexableField field : doc.indexableFields()) {
      if (field.fieldType() == FacetField.TYPE) {
        FacetField facetField = (FacetField) field;
        FacetsConfig.DimConfig dimConfig = facetsConfig.getDimConfig(field.name());
        String indexedFieldName = dimConfig.indexedFieldName;
        List<FacetField> fields = byField.get(indexedFieldName);
        if (fields == null) {
          fields = new ArrayList<FacetField>();
          byField.put(indexedFieldName, fields);
        }
        fields.add(facetField);
      }

      if (field.fieldType() == SortedSetDocValuesFacetField.TYPE) {
        SortedSetDocValuesFacetField facetField = (SortedSetDocValuesFacetField) field;
        FacetsConfig.DimConfig dimConfig = facetsConfig.getDimConfig(field.name());
        String indexedFieldName = dimConfig.indexedFieldName;
        List<SortedSetDocValuesFacetField> fields = dvByField.get(indexedFieldName);
        if (fields == null) {
          fields = new ArrayList<SortedSetDocValuesFacetField>();
          dvByField.put(indexedFieldName, fields);
        }
        fields.add(facetField);
      }
    }

    List<Field> addedIndexedFields = new ArrayList<Field>();
    List<Field> addedStoredFields = new ArrayList<Field>();

    processFacetFields(byField, addedIndexedFields, addedStoredFields);
    processSSDVFacetFields(dvByField, addedIndexedFields, addedStoredFields);

    //System.out.println("add stored: " + addedStoredFields);

    final List<IndexableField> allIndexedFields = new ArrayList<IndexableField>();
    for(IndexableField field : doc.indexableFields()) {
      IndexableFieldType ft = field.fieldType();
      if (ft != FacetField.TYPE && ft != SortedSetDocValuesFacetField.TYPE) {
        allIndexedFields.add(field);
      }
    }
    allIndexedFields.addAll(addedIndexedFields);

    final List<StorableField> allStoredFields = new ArrayList<StorableField>();
    for(StorableField field : doc.storableFields()) {
      allStoredFields.add(field);
    }
    allStoredFields.addAll(addedStoredFields);

    //System.out.println("all indexed: " + allIndexedFields);
    //System.out.println("all stored: " + allStoredFields);

    super.addDocument(new IndexDocument() {
        @Override
        public Iterable<IndexableField> indexableFields() {
          return allIndexedFields;
        }

        @Override
        public Iterable<StorableField> storableFields() {
          return allStoredFields;
        }
      });
  }

  private void processFacetFields(Map<String,List<FacetField>> byField, List<Field> addedIndexedFields, List<Field> addedStoredFields) throws IOException {

    for(Map.Entry<String,List<FacetField>> ent : byField.entrySet()) {

      // nocommit maybe we can somehow catch singleValued
      // dim appearing more than once?

      String indexedFieldName = ent.getKey();
      //System.out.println("  fields=" + ent.getValue());

      IntsRef ordinals = new IntsRef(32);
      for(FacetField facetField : ent.getValue()) {

        FacetsConfig.DimConfig ft = facetsConfig.getDimConfig(facetField.dim);
        if (facetField.path.length > 1 && ft.hierarchical == false) {
          throw new IllegalArgumentException("dimension \"" + facetField.dim + "\" is not hierarchical yet has " + facetField.path.length + " components");
        }
      
        FacetLabel cp = FacetLabel.create(facetField.dim, facetField.path);

        int ordinal = taxoWriter.addCategory(cp);
        ordinals.ints[ordinals.length++] = ordinal;
        //System.out.println("  add cp=" + cp);

        if (ft.hierarchical && ft.multiValued) {
          // Add all parents too:
          int parent = taxoWriter.getParent(ordinal);
          while (parent > 0) {
            if (ordinals.ints.length == ordinals.length) {
              ordinals.grow(ordinals.length+1);
            }
            ordinals.ints[ordinals.length++] = parent;
            parent = taxoWriter.getParent(parent);
          }
        }

        // Drill down:
        for(int i=2;i<=cp.length;i++) {
          addedIndexedFields.add(new StringField(indexedFieldName, pathToString(cp.components, i), Field.Store.NO));
        }
      }

      // Facet counts:
      // DocValues are considered stored fields:
      addedStoredFields.add(new BinaryDocValuesField(indexedFieldName, dedupAndEncode(ordinals)));
    }
  }

  private void processSSDVFacetFields(Map<String,List<SortedSetDocValuesFacetField>> byField, List<Field> addedIndexedFields, List<Field> addedStoredFields) throws IOException {
    //System.out.println("process SSDV: " + byField);
    for(Map.Entry<String,List<SortedSetDocValuesFacetField>> ent : byField.entrySet()) {

      String indexedFieldName = ent.getKey();
      //System.out.println("  field=" + indexedFieldName);

      for(SortedSetDocValuesFacetField facetField : ent.getValue()) {
        FacetLabel cp = new FacetLabel(facetField.dim, facetField.label);
        String fullPath = pathToString(cp.components, cp.length);
        //System.out.println("add " + fullPath);

        // For facet counts:
        addedStoredFields.add(new SortedSetDocValuesField(indexedFieldName, new BytesRef(fullPath)));

        // For drill-down:
        addedIndexedFields.add(new StringField(indexedFieldName, fullPath, Field.Store.NO));
      }
    }
  }

  /** We can open this up if/when we really need
   *  pluggability on the encoding. */
  private final BytesRef dedupAndEncode(IntsRef ordinals) {
    Arrays.sort(ordinals.ints, ordinals.offset, ordinals.length);
    byte[] bytes = new byte[5*ordinals.length];
    int lastOrd = -1;
    int upto = 0;
    for(int i=0;i<ordinals.length;i++) {
      int ord = ordinals.ints[ordinals.offset+i];
      // ord could be == lastOrd, so we must dedup:
      if (ord > lastOrd) {
        int delta;
        if (lastOrd == -1) {
          delta = ord;
        } else {
          delta = ord - lastOrd;
        }
        if ((delta & ~0x7F) == 0) {
          bytes[upto] = (byte) delta;
          upto++;
        } else if ((delta & ~0x3FFF) == 0) {
          bytes[upto] = (byte) (0x80 | ((delta & 0x3F80) >> 7));
          bytes[upto + 1] = (byte) (delta & 0x7F);
          upto += 2;
        } else if ((delta & ~0x1FFFFF) == 0) {
          bytes[upto] = (byte) (0x80 | ((delta & 0x1FC000) >> 14));
          bytes[upto + 1] = (byte) (0x80 | ((delta & 0x3F80) >> 7));
          bytes[upto + 2] = (byte) (delta & 0x7F);
          upto += 3;
        } else if ((delta & ~0xFFFFFFF) == 0) {
          bytes[upto] = (byte) (0x80 | ((delta & 0xFE00000) >> 21));
          bytes[upto + 1] = (byte) (0x80 | ((delta & 0x1FC000) >> 14));
          bytes[upto + 2] = (byte) (0x80 | ((delta & 0x3F80) >> 7));
          bytes[upto + 3] = (byte) (delta & 0x7F);
          upto += 4;
        } else {
          bytes[upto] = (byte) (0x80 | ((delta & 0xF0000000) >> 28));
          bytes[upto + 1] = (byte) (0x80 | ((delta & 0xFE00000) >> 21));
          bytes[upto + 2] = (byte) (0x80 | ((delta & 0x1FC000) >> 14));
          bytes[upto + 3] = (byte) (0x80 | ((delta & 0x3F80) >> 7));
          bytes[upto + 4] = (byte) (delta & 0x7F);
          upto += 5;
        }
        lastOrd = ord;
      }
    }
    return new BytesRef(bytes, 0, upto);
  }

  // nocommit move these constants / methods to Util?

  // Joins the path components together:
  private static final char DELIM_CHAR = '\u001F';

  // Escapes any occurrence of the path component inside the label:
  private static final char ESCAPE_CHAR = '\u001E';

  /** Turns a path into a string without stealing any
   *  characters. */
  public static String pathToString(String dim, String[] path) {
    String[] fullPath = new String[1+path.length];
    fullPath[0] = dim;
    System.arraycopy(path, 0, fullPath, 1, path.length);
    return pathToString(fullPath, fullPath.length);
  }

  public static String pathToString(String[] path) {
    return pathToString(path, path.length);
  }

  public static String pathToString(String[] path, int length) {
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<length;i++) {
      String s = path[i];
      int numChars = s.length();
      for(int j=0;j<numChars;j++) {
        char ch = s.charAt(j);
        if (ch == DELIM_CHAR || ch == ESCAPE_CHAR) {
          sb.append(ESCAPE_CHAR);
        }
        sb.append(ch);
      }
      sb.append(DELIM_CHAR);
    }

    // Trim off last DELIM_CHAR:
    sb.setLength(sb.length()-1);
    return sb.toString();
  }

  /** Turns a result from previous call to {@link
   *  #pathToString} back into the original {@code String[]}
   *  without stealing any characters. */
  public static String[] stringToPath(String s) {
    List<String> parts = new ArrayList<String>();
    int length = s.length();
    char[] buffer = new char[length];

    int upto = 0;
    boolean lastEscape = false;
    for(int i=0;i<length;i++) {
      char ch = s.charAt(i);
      if (lastEscape) {
        buffer[upto++] = ch;
        lastEscape = false;
      } else if (ch == ESCAPE_CHAR) {
        lastEscape = true;
      } else if (ch == DELIM_CHAR) {
        parts.add(new String(buffer, 0, upto));
        upto = 0;
      } else {
        buffer[upto++] = ch;
      }
    }
    assert !lastEscape;
    return parts.toArray(new String[parts.size()]);
  }
}