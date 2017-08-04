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

package org.apache.solr.search;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.document.LazyDocument;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrDocumentBase;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.AbstractEnumField;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class of {@link org.apache.solr.search.SolrIndexSearcher} for stored Document related matters
 * including DocValue substitutions.
 */
public class SolrDocumentFetcher {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrIndexSearcher searcher;

  private final boolean enableLazyFieldLoading;

  private final SolrCache<Integer,Document> documentCache;

  /** Contains the names/patterns of all docValues=true,stored=false fields in the schema. */
  private final Set<String> allNonStoredDVs;

  /** Contains the names/patterns of all docValues=true,stored=false,useDocValuesAsStored=true fields in the schema. */
  private final Set<String> nonStoredDVsUsedAsStored;

  /** Contains the names/patterns of all docValues=true,stored=false fields, excluding those that are copyField targets in the schema. */
  private final Set<String> nonStoredDVsWithoutCopyTargets;

  private static int largeValueLengthCacheThreshold = Integer.getInteger("solr.largeField.cacheThreshold", 512 * 1024); // internal setting

  private final Set<String> largeFields;

  private Collection<String> storedHighlightFieldNames; // lazy populated; use getter

  SolrDocumentFetcher(SolrIndexSearcher searcher, SolrConfig solrConfig, boolean cachingEnabled) {
    this.searcher = searcher;
    this.enableLazyFieldLoading = solrConfig.enableLazyFieldLoading;
    if (cachingEnabled) {
      documentCache = solrConfig.documentCacheConfig == null ? null : solrConfig.documentCacheConfig.newInstance();
    } else {
      documentCache = null;
    }

    final Set<String> nonStoredDVsUsedAsStored = new HashSet<>();
    final Set<String> allNonStoredDVs = new HashSet<>();
    final Set<String> nonStoredDVsWithoutCopyTargets = new HashSet<>();
    final Set<String> storedLargeFields = new HashSet<>();

    for (FieldInfo fieldInfo : searcher.getFieldInfos()) { // can find materialized dynamic fields, unlike using the Solr IndexSchema.
      final SchemaField schemaField = searcher.getSchema().getFieldOrNull(fieldInfo.name);
      if (schemaField == null) {
        continue;
      }
      if (!schemaField.stored() && schemaField.hasDocValues()) {
        if (schemaField.useDocValuesAsStored()) {
          nonStoredDVsUsedAsStored.add(fieldInfo.name);
        }
        allNonStoredDVs.add(fieldInfo.name);
        if (!searcher.getSchema().isCopyFieldTarget(schemaField)) {
          nonStoredDVsWithoutCopyTargets.add(fieldInfo.name);
        }
      }
      if (schemaField.stored() && schemaField.isLarge()) {
        storedLargeFields.add(schemaField.getName());
      }
    }

    this.nonStoredDVsUsedAsStored = Collections.unmodifiableSet(nonStoredDVsUsedAsStored);
    this.allNonStoredDVs = Collections.unmodifiableSet(allNonStoredDVs);
    this.nonStoredDVsWithoutCopyTargets = Collections.unmodifiableSet(nonStoredDVsWithoutCopyTargets);
    this.largeFields = Collections.unmodifiableSet(storedLargeFields);
  }

  public boolean isLazyFieldLoadingEnabled() {
    return enableLazyFieldLoading;
  }

  public SolrCache<Integer, Document> getDocumentCache() {
    return documentCache;
  }

  /**
   * Returns a collection of the names of all stored fields which can be highlighted the index reader knows about.
   */
  public Collection<String> getStoredHighlightFieldNames() {
    synchronized (this) {
      if (storedHighlightFieldNames == null) {
        storedHighlightFieldNames = new LinkedList<>();
        for (FieldInfo fieldInfo : searcher.getFieldInfos()) {
          final String fieldName = fieldInfo.name;
          try {
            SchemaField field = searcher.getSchema().getField(fieldName);
            if (field.stored() && ((field.getType() instanceof org.apache.solr.schema.TextField)
                || (field.getType() instanceof org.apache.solr.schema.StrField))) {
              storedHighlightFieldNames.add(fieldName);
            }
          } catch (RuntimeException e) { // getField() throws a SolrException, but it arrives as a RuntimeException
            log.warn("Field [{}] found in index, but not defined in schema.", fieldName);
          }
        }
      }
      return storedHighlightFieldNames;
    }
  }

  /** @see SolrIndexSearcher#doc(int) */
  public Document doc(int docId) throws IOException {
    return doc(docId, (Set<String>) null);
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   * <p>
   * <b>NOTE</b>: the document will have all fields accessible, but if a field filter is provided, only the provided
   * fields will be loaded (the remainder will be available lazily).
   *
   * @see SolrIndexSearcher#doc(int, Set)
   */
  public Document doc(int i, Set<String> fields) throws IOException {
    Document d;
    if (documentCache != null) {
      d = documentCache.get(i);
      if (d != null) return d;
    }

    final DirectoryReader reader = searcher.getIndexReader();
    if (documentCache != null && !enableLazyFieldLoading) {
      // we do not filter the fields in this case because that would return an incomplete document which would
      // be eventually cached. The alternative would be to read the stored fields twice; once with the fields
      // and then without for caching leading to a performance hit
      // see SOLR-8858 for related discussion
      fields = null;
    }
    final SolrDocumentStoredFieldVisitor visitor = new SolrDocumentStoredFieldVisitor(fields, reader, i);
    reader.document(i, visitor);
    d = visitor.getDocument();

    if (documentCache != null) {
      documentCache.put(i, d);
    }

    return d;
  }

  /** {@link StoredFieldVisitor} which loads the specified fields eagerly (or all if null).
   * If {@link #enableLazyFieldLoading} then the rest get special lazy field entries.  Designated "large"
   * fields will always get a special field entry. */
  private class SolrDocumentStoredFieldVisitor extends DocumentStoredFieldVisitor {
    private final Document doc;
    private final LazyDocument lazyFieldProducer; // arguably a better name than LazyDocument; at least how we use it here
    private final int docId;
    private final boolean addLargeFieldsLazily;

    SolrDocumentStoredFieldVisitor(Set<String> toLoad, IndexReader reader, int docId) {
      super(toLoad);
      this.docId = docId;
      this.doc = getDocument();
      this.lazyFieldProducer = toLoad != null && enableLazyFieldLoading ? new LazyDocument(reader, docId) : null;
      this.addLargeFieldsLazily = (documentCache != null && !largeFields.isEmpty());
      //TODO can we return Status.STOP after a val is loaded and we know there are no other fields of interest?
      //    When: toLoad is one single-valued field, no lazyFieldProducer
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      Status status = super.needsField(fieldInfo);
      assert status != Status.STOP : "Status.STOP not supported or expected";
      if (addLargeFieldsLazily && largeFields.contains(fieldInfo.name)) { // load "large" fields using this lazy mechanism
        if (lazyFieldProducer != null || status == Status.YES) {
          doc.add(new LargeLazyField(fieldInfo.name, docId));
        }
        return Status.NO;
      }
      if (status == Status.NO && lazyFieldProducer != null) { // lazy
        doc.add(lazyFieldProducer.getField(fieldInfo));
      }
      return status;
    }
  }

  /** @see SolrIndexSearcher#doc(int, StoredFieldVisitor) */
  public void doc(int docId, StoredFieldVisitor visitor) throws IOException {
    if (documentCache != null) {
      Document cached = documentCache.get(docId);
      if (cached != null) {
        visitFromCached(cached, visitor);
        return;
      }
    }
    searcher.getIndexReader().document(docId, visitor);
  }

  /** Executes a stored field visitor against a hit from the document cache */
  private void visitFromCached(Document document, StoredFieldVisitor visitor) throws IOException {
    for (IndexableField f : document) {
      final FieldInfo info = searcher.getFieldInfos().fieldInfo(f.name());
      final StoredFieldVisitor.Status needsField = visitor.needsField(info);
      if (needsField == StoredFieldVisitor.Status.STOP) return;
      if (needsField == StoredFieldVisitor.Status.NO) continue;
      BytesRef binaryValue = f.binaryValue();
      if (binaryValue != null) {
        visitor.binaryField(info, toByteArrayUnwrapIfPossible(binaryValue));
        continue;
      }
      Number numericValue = f.numericValue();
      if (numericValue != null) {
        if (numericValue instanceof Double) {
          visitor.doubleField(info, numericValue.doubleValue());
        } else if (numericValue instanceof Integer) {
          visitor.intField(info, numericValue.intValue());
        } else if (numericValue instanceof Float) {
          visitor.floatField(info, numericValue.floatValue());
        } else if (numericValue instanceof Long) {
          visitor.longField(info, numericValue.longValue());
        } else {
          throw new AssertionError();
        }
        continue;
      }
      // must be String
      if (f instanceof LargeLazyField) { // optimization to avoid premature string conversion
        visitor.stringField(info, toByteArrayUnwrapIfPossible(((LargeLazyField) f).readBytes()));
      } else {
        visitor.stringField(info, f.stringValue().getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private byte[] toByteArrayUnwrapIfPossible(BytesRef bytesRef) {
    if (bytesRef.offset == 0 && bytesRef.bytes.length == bytesRef.length) {
      return bytesRef.bytes;
    } else {
      return Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }
  }

  /** Unlike LazyDocument.LazyField, we (a) don't cache large values, and (b) provide access to the byte[]. */
  class LargeLazyField implements IndexableField {

    final String name;
    final int docId;
    // synchronize on 'this' to access:
    BytesRef cachedBytes; // we only conditionally populate this if it's big enough

    private LargeLazyField(String name, int docId) {
      this.name = name;
      this.docId = docId;
    }

    @Override
    public String toString() {
      return fieldType().toString() + "<" + name() + ">"; // mimic Field.java
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public IndexableFieldType fieldType() {
      return searcher.getSchema().getField(name());
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
      return analyzer.tokenStream(name(), stringValue()); // or we could throw unsupported exception?
    }
    /** (for tests) */
    synchronized boolean hasBeenLoaded() {
      return cachedBytes != null;
    }

    @Override
    public synchronized String stringValue() {
      try {
        return readBytes().utf8ToString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    synchronized BytesRef readBytes() throws IOException {
      if (cachedBytes != null) {
        return cachedBytes;
      } else {
        BytesRef bytesRef = new BytesRef();
        searcher.getIndexReader().document(docId, new StoredFieldVisitor() {
          boolean done = false;
          @Override
          public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (done) {
              return Status.STOP;
            }
            return fieldInfo.name.equals(name()) ? Status.YES : Status.NO;
          }

          @Override
          public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
            bytesRef.bytes = value;
            bytesRef.length = value.length;
            done = true;
          }

          @Override
          public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            throw new UnsupportedOperationException("'large' binary fields are not (yet) supported");
          }
        });
        if (bytesRef.length < largeValueLengthCacheThreshold) {
          return cachedBytes = bytesRef;
        } else {
          return bytesRef;
        }
      }
    }

    @Override
    public BytesRef binaryValue() {
      return null;
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public Number numericValue() {
      return null;
    }
  }

  /**
   * This will fetch and add the docValues fields to a given SolrDocument/SolrInputDocument
   *
   * @param doc
   *          A SolrDocument or SolrInputDocument instance where docValues will be added
   * @param docid
   *          The lucene docid of the document to be populated
   * @param fields
   *          The list of docValues fields to be decorated
   */
  public void decorateDocValueFields(@SuppressWarnings("rawtypes") SolrDocumentBase doc, int docid, Set<String> fields)
      throws IOException {
    final List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
    final int subIndex = ReaderUtil.subIndex(docid, leafContexts);
    final int localId = docid - leafContexts.get(subIndex).docBase;
    final LeafReader leafReader = leafContexts.get(subIndex).reader();
    for (String fieldName : fields) {
      final SchemaField schemaField = searcher.getSchema().getFieldOrNull(fieldName);
      if (schemaField == null || !schemaField.hasDocValues() || doc.containsKey(fieldName)) {
        log.warn("Couldn't decorate docValues for field: [{}], schemaField: [{}]", fieldName, schemaField);
        continue;
      }
      FieldInfo fi = searcher.getFieldInfos().fieldInfo(fieldName);
      if (fi == null) {
        continue; // Searcher doesn't have info about this field, hence ignore it.
      }
      final DocValuesType dvType = fi.getDocValuesType();
      switch (dvType) {
        case NUMERIC:
          final NumericDocValues ndv = leafReader.getNumericDocValues(fieldName);
          if (ndv == null) {
            continue;
          }
          Long val;
          if (ndv.advanceExact(localId)) {
            val = ndv.longValue();
          } else {
            continue;
          }
          Object newVal = val;
          if (schemaField.getType().isPointField()) {
            // TODO: Maybe merge PointField with TrieFields here
            NumberType type = schemaField.getType().getNumberType();
            switch (type) {
              case INTEGER:
                newVal = val.intValue();
                break;
              case LONG:
                newVal = val.longValue();
                break;
              case FLOAT:
                newVal = Float.intBitsToFloat(val.intValue());
                break;
              case DOUBLE:
                newVal = Double.longBitsToDouble(val);
                break;
              case DATE:
                newVal = new Date(val);
                break;
              default:
                throw new AssertionError("Unexpected PointType: " + type);
            }
          } else {
            if (schemaField.getType() instanceof TrieIntField) {
              newVal = val.intValue();
            } else if (schemaField.getType() instanceof TrieFloatField) {
              newVal = Float.intBitsToFloat(val.intValue());
            } else if (schemaField.getType() instanceof TrieDoubleField) {
              newVal = Double.longBitsToDouble(val);
            } else if (schemaField.getType() instanceof TrieDateField) {
              newVal = new Date(val);
            } else if (schemaField.getType() instanceof AbstractEnumField) {
              newVal = ((AbstractEnumField)schemaField.getType()).getEnumMapping().intValueToStringValue(val.intValue());
            }
          }
          doc.addField(fieldName, newVal);
          break;
        case BINARY:
          BinaryDocValues bdv = leafReader.getBinaryDocValues(fieldName);
          if (bdv == null) {
            continue;
          }
          BytesRef value;
          if (bdv.advanceExact(localId)) {
            value = BytesRef.deepCopyOf(bdv.binaryValue());
          } else {
            continue;
          }
          doc.addField(fieldName, value);
          break;
        case SORTED:
          SortedDocValues sdv = leafReader.getSortedDocValues(fieldName);
          if (sdv == null) {
            continue;
          }
          if (sdv.advanceExact(localId)) {
            final BytesRef bRef = sdv.binaryValue();
            // Special handling for Boolean fields since they're stored as 'T' and 'F'.
            if (schemaField.getType() instanceof BoolField) {
              doc.addField(fieldName, schemaField.getType().toObject(schemaField, bRef));
            } else {
              doc.addField(fieldName, bRef.utf8ToString());
            }
          }
          break;
        case SORTED_NUMERIC:
          final SortedNumericDocValues numericDv = leafReader.getSortedNumericDocValues(fieldName);
          final NumberType type = schemaField.getType().getNumberType();
          if (numericDv != null) {
            if (numericDv.advance(localId) == localId) {
              final List<Object> outValues = new ArrayList<Object>(numericDv.docValueCount());
              for (int i = 0; i < numericDv.docValueCount(); i++) {
                long number = numericDv.nextValue();
                switch (type) {
                  case INTEGER:
                    final int raw = (int)number;
                    if (schemaField.getType() instanceof AbstractEnumField) {
                      outValues.add(((AbstractEnumField)schemaField.getType()).getEnumMapping().intValueToStringValue(raw));
                    } else {
                      outValues.add(raw);
                    }
                    break;
                  case LONG:
                    outValues.add(number);
                    break;
                  case FLOAT:
                    outValues.add(NumericUtils.sortableIntToFloat((int)number));
                    break;
                  case DOUBLE:
                    outValues.add(NumericUtils.sortableLongToDouble(number));
                    break;
                  case DATE:
                    outValues.add(new Date(number));
                    break;
                  default:
                    throw new AssertionError("Unexpected PointType: " + type);
                }
              }
              assert outValues.size() > 0;
              doc.addField(fieldName, outValues);
            }
          }
        case SORTED_SET:
          final SortedSetDocValues values = leafReader.getSortedSetDocValues(fieldName);
          if (values != null && values.getValueCount() > 0) {
            if (values.advance(localId) == localId) {
              final List<Object> outValues = new LinkedList<>();
              for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                value = values.lookupOrd(ord);
                outValues.add(schemaField.getType().toObject(schemaField, value));
              }
              assert outValues.size() > 0;
              doc.addField(fieldName, outValues);
            }
          }
        case NONE:
          break;
      }
    }
  }

  /**
   * Returns an unmodifiable set of non-stored docValues field names.
   *
   * @param onlyUseDocValuesAsStored
   *          If false, returns all non-stored docValues. If true, returns only those non-stored docValues which have
   *          the {@link SchemaField#useDocValuesAsStored()} flag true.
   */
  public Set<String> getNonStoredDVs(boolean onlyUseDocValuesAsStored) {
    return onlyUseDocValuesAsStored ? nonStoredDVsUsedAsStored : allNonStoredDVs;
  }

  /**
   * Returns an unmodifiable set of names of non-stored docValues fields, except those that are targets of a copy field.
   */
  public Set<String> getNonStoredDVsWithoutCopyTargets() {
    return nonStoredDVsWithoutCopyTargets;
  }

}
