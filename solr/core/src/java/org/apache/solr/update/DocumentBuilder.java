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
package org.apache.solr.update;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Sets;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocumentBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * Builds a Lucene {@link Document} from a {@link SolrInputDocument}.
 */
public class DocumentBuilder {

  // accessible only for tests
  static int MIN_LENGTH_TO_MOVE_LAST = Integer.getInteger("solr.docBuilder.minLengthToMoveLast", 4*1024); // internal setting

  /**
   * Add a field value to a given document.
   * @param doc Document that the field needs to be added to
   * @param field The schema field object for the field
   * @param val The value for the field to be added
   * @param forInPlaceUpdate Whether the field is to be added for in-place update. If true,
   *        only numeric docValues based fields are added to the document. This can be true
   *        when constructing a Lucene document for writing an in-place update, and we don't need
   *        presence of non-updatable fields (non NDV) in such a document.
   */
  private static void addField(Document doc, SchemaField field, Object val,
      boolean forInPlaceUpdate) {
    if (val instanceof IndexableField) {
      if (forInPlaceUpdate) {
        assert val instanceof NumericDocValuesField: "Expected in-place update to be done on"
            + " NDV fields only.";
      }
      doc.add((IndexableField)val);
      return;
    }
    for (IndexableField f : field.getType().createFields(field, val)) {
      if (f != null) { // null fields are not added
        // HACK: workaround for SOLR-9809
        // even though at this point in the code we know the field is single valued and DV only
        // TrieField.createFields() may still return (usless) IndexableField instances that are not
        // NumericDocValuesField instances.
        //
        // once SOLR-9809 is resolved, we should be able to replace this conditional with...
        //    assert f instanceof NumericDocValuesField
        if (forInPlaceUpdate) {
          if (f instanceof NumericDocValuesField) {
            doc.add(f);
          }
        } else {
          doc.add(f);
        }
      }
    }
  }
  
  private static String getID( SolrInputDocument doc, IndexSchema schema )
  {
    String id = "";
    SchemaField sf = schema.getUniqueKeyField();
    if( sf != null ) {
      id = "[doc="+doc.getFieldValue( sf.getName() )+"] ";
    }
    return id;
  }

  /**
   * @see DocumentBuilder#toDocument(SolrInputDocument, IndexSchema, boolean, boolean)
   */
  public static Document toDocument( SolrInputDocument doc, IndexSchema schema )
  {
    return toDocument(doc, schema, false, true);
  }
  
  /**
   * Convert a SolrInputDocument to a lucene Document.
   * 
   * This function should go elsewhere.  This builds the Document without an
   * extra Map&lt;&gt; checking for multiple values.  For more discussion, see:
   * http://www.nabble.com/Re%3A-svn-commit%3A-r547493---in--lucene-solr-trunk%3A-.--src-java-org-apache-solr-common--src-java-org-apache-solr-schema--src-java-org-apache-solr-update--src-test-org-apache-solr-common--tf3931539.html
   * 
   * TODO: /!\ NOTE /!\ This semantics of this function are still in flux.  
   * Something somewhere needs to be able to fill up a SolrDocument from
   * a lucene document - this is one place that may happen.  It may also be
   * moved to an independent function
   * 
   * @since solr 1.3
   * 
   * @param doc SolrInputDocument from which the document has to be built
   * @param schema Schema instance
   * @param forInPlaceUpdate Whether the output document would be used for an in-place update or not. When this is true,
   *        default fields values and copy fields targets are not populated.
   * @param ignoreNestedDocs if nested child documents should be ignored.  If false then an exception will be thrown.
   * @return Built Lucene document
   */
  public static Document toDocument(SolrInputDocument doc, IndexSchema schema, boolean forInPlaceUpdate, boolean ignoreNestedDocs) {
    if (!ignoreNestedDocs && doc.hasChildDocuments()) {
      throw unexpectedNestedDocException(schema, forInPlaceUpdate);
    }

    final SchemaField uniqueKeyField = schema.getUniqueKeyField();
    final String uniqueKeyFieldName = null == uniqueKeyField ? null : uniqueKeyField.getName();
    
    Document out = new Document();
    Set<String> usedFields = Sets.newHashSet();
    
    // Load fields from SolrDocument to Document
    for( SolrInputField field : doc ) {

      if (field.getFirstValue() instanceof SolrDocumentBase) {
        if (ignoreNestedDocs) {
          continue;
        }
        throw unexpectedNestedDocException(schema, forInPlaceUpdate);
      }

      String name = field.getName();
      SchemaField sfield = schema.getFieldOrNull(name);
      boolean used = false;
      
      // Make sure it has the correct number
      if( sfield!=null && !sfield.multiValued() && field.getValueCount() > 1 ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"multiple values encountered for non multiValued field " + 
              sfield.getName() + ": " +field.getValue() );
      }

      List<CopyField> copyFields = schema.getCopyFieldsList(name);
      if( copyFields.size() == 0 ) copyFields = null;

      // load each field value
      boolean hasField = false;
      try {
        @SuppressWarnings({"rawtypes"})
        Iterator it = field.iterator();
        while (it.hasNext()) {
          Object v = it.next();
          if( v == null ) {
            continue;
          }
          hasField = true;
          if (sfield != null) {
            used = true;
            addField(out, sfield, v,
                     name.equals(uniqueKeyFieldName) ? false : forInPlaceUpdate);
            // record the field as having a value
            usedFields.add(sfield.getName());
          }
  
          // Check if we should copy this field value to any other fields.
          // This could happen whether it is explicit or not.
          if (copyFields != null) {
            // Do not copy this field if this document is to be used for an in-place update,
            // and this is the uniqueKey field (because the uniqueKey can't change so no need to "update" the copyField).
            if ( ! (forInPlaceUpdate && name.equals(uniqueKeyFieldName)) ) {
              for (CopyField cf : copyFields) {
                SchemaField destinationField = cf.getDestination();

                final boolean destHasValues = usedFields.contains(destinationField.getName());

                // check if the copy field is a multivalued or not
                if (!destinationField.multiValued() && destHasValues) {
                  throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                      "Multiple values encountered for non multiValued copy field " +
                      destinationField.getName() + ": " + v);
                }

                used = true;

                // Perhaps trim the length of a copy field
                Object val = v;
                if( val instanceof CharSequence && cf.getMaxChars() > 0 ) {
                    val = cf.getLimitedValue(val.toString());
                }

                addField(out, destinationField, val,
                         destinationField.getName().equals(uniqueKeyFieldName) ? false : forInPlaceUpdate);
                // record the field as having a value
                usedFields.add(destinationField.getName());
              }
            }
          }
        }
      }
      catch( SolrException ex ) {
        throw new SolrException(SolrException.ErrorCode.getErrorCode(ex.code()),
            "ERROR: "+getID(doc, schema)+"Error adding field '" + 
              field.getName() + "'='" +field.getValue()+"' msg=" + ex.getMessage(), ex );
      }
      catch( Exception ex ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"Error adding field '" + 
              field.getName() + "'='" +field.getValue()+"' msg=" + ex.getMessage(), ex );
      }
      
      // make sure the field was used somehow...
      if( !used && hasField ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"unknown field '" +name + "'");
      }
    }
    
        
    // Now validate required fields or add default values
    // fields with default values are defacto 'required'

    // Note: We don't need to add required fields if this document is to be used for
    // in-place updates, since this validation and population of required fields would've happened
    // during the full indexing initially.
    if (!forInPlaceUpdate) {
      for (SchemaField field : schema.getRequiredFields()) {
        if (out.getField(field.getName() ) == null) {
          if (field.getDefaultValue() != null) {
            addField(out, field, field.getDefaultValue(), false);
          } 
          else {
            String msg = getID(doc, schema) + "missing required field: " + field.getName();
            throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, msg );
          }
        }
      }
    }

    if (!forInPlaceUpdate) {
      moveLargestFieldLast(out);
    }
    
    collapseMultiBinary(out.iterator(), schema);

    return out;
  }

  private static class CollapseEntry {
    private final BinaryDocValuesField bdv;
    private SortedSet<BytesRef> vals;
    private CollapseEntry(BinaryDocValuesField bdv) {
      this(bdv, null);
    }
    private CollapseEntry(BinaryDocValuesField bdv, SortedSet<BytesRef> vals) {
      this.bdv = bdv;
      this.vals = vals;
    }
    private boolean add(BinaryDocValuesField bdv) {
      if (vals == null) {
        vals = new TreeSet<>();
        vals.add(this.bdv.binaryValue());
      }
      return vals.add(bdv.binaryValue());
    }
  }
  private static Map<String,CollapseEntry> initMultiBinary(Iterator<IndexableField> iter, IndexSchema schema) {
    String firstName = null;
    BinaryDocValuesField first = null;
    SortedSet<BytesRef> firstVals = null;
    while (iter.hasNext()) {
      IndexableField next = iter.next();
      if (next instanceof BinaryDocValuesField) {
        final String nextName = next.name();
        if (first == null) {
          if (schema.getField(nextName).multiValued()) {
            // if null, assume multiValued. See note below.
            firstName = nextName;
            first = (BinaryDocValuesField) next;
          }
        } else if (firstName.equals(nextName)) {
          iter.remove();
          if (firstVals == null) {
            firstVals = new TreeSet<>();
            firstVals.add(first.binaryValue());
          }
          firstVals.add(((BinaryDocValuesField)next).binaryValue());
        } else if (iter.hasNext()) {
          final Map<String,CollapseEntry> ret = new HashMap<>();
          ret.put(firstName, new CollapseEntry(first, firstVals));
          ret.put(nextName, new CollapseEntry((BinaryDocValuesField)next, null));
          return ret;
        }
      }
    }
    if (firstVals != null) {
      collapse(first, firstVals);
    }
    return null;
  }

  private static void collapse(BinaryDocValuesField bdv, SortedSet<BytesRef> vals) {
    if (vals == null || vals.size() == 1) { // vals.size()==1 could happen if duplicates of a single value
      final BytesRef val = bdv.binaryValue();
      final int length = val.length;
      final byte[] bs = new byte[length + MAX_VINT_BYTES];
      final int headerLength = writeVInt(length, bs, 0);
      System.arraycopy(val.bytes, val.offset, bs, headerLength, length);
      val.bytes = bs;
      val.offset = 0;
      val.length += headerLength;
    } else {
      final int size = vals.size();
      BytesRef[] refs = new BytesRef[size];
      Iterator<BytesRef>iter = vals.iterator();
      int allocate = size * MAX_VINT_BYTES; // initialize to cover max possible header overhead
      for (int i = 0; i < size; i++) {
        BytesRef ref = iter.next();
        allocate += ref.length;
        refs[i] = ref;
      }
      int offset = 0;
      final byte[] bs = new byte[allocate];
      for (BytesRef val : refs) {
        final int length = val.length;
        offset = writeVInt(val.length, bs, offset);
        System.arraycopy(val.bytes, val.offset, bs, offset, length);
        offset += length;
      }
      final BytesRef combined = bdv.binaryValue();
      combined.bytes = bs;
      combined.offset = 0;
      combined.length = offset;
    }
  }

  private static final int MAX_VINT_BYTES = 5;
  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the length of a
   * collection/array/map In most of the cases the length can be represented in one byte (length &lt; 127) so it saves 3
   * bytes/object
   */
  public static int writeVInt(int val, byte[] bs, int offset) {
    while ((val & ~0x7F) != 0) {
      bs[offset++] = ((byte) ((val & 0x7f) | 0x80));
      val >>>= 7;
    }
    bs[offset++] = ((byte) val);
    return offset;
  }

  private static void collapseMultiBinary(Iterator<IndexableField> iter, IndexSchema schema) {
    Map<String,CollapseEntry> multiBdvFields = initMultiBinary(iter, schema);
    if (multiBdvFields == null) {
      return;
    }
    do {
      IndexableField next = iter.next();
      if (next instanceof BinaryDocValuesField) {
        final String fieldName = next.name();
        CollapseEntry initial = multiBdvFields.get(next.name());
        if (initial != null) {
          iter.remove();
          initial.add((BinaryDocValuesField)next);
        } else if (schema.getField(fieldName).multiValued()) {
          multiBdvFields.put(fieldName, new CollapseEntry((BinaryDocValuesField) next, null));
        }
      }
    } while (iter.hasNext());
    for (CollapseEntry e : multiBdvFields.values()) {
      collapse(e.bdv, e.vals);
    }
  }

  private static SolrException unexpectedNestedDocException(IndexSchema schema, boolean forInPlaceUpdate) {
    if (! schema.isUsableForChildDocs()) {
      return new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unable to index docs with children: the schema must " +
              "include definitions for both a uniqueKey field and the '" + IndexSchema.ROOT_FIELD_NAME +
              "' field, using the exact same fieldType");
    } else if (forInPlaceUpdate) {
      return new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Unable to index docs with children: for an in-place update, just provide the doc by itself");
    } else {
      return new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "A document unexpectedly contained nested child documents");
    }
  }

  /** Move the largest stored field last, because Lucene can avoid loading that one if it's not needed. */
  private static void moveLargestFieldLast(Document doc) {
    String largestField = null;
    int largestFieldLen = -1;
    boolean largestIsLast = true;
    for (IndexableField field : doc) {
      if (!field.fieldType().stored()) {
        continue;
      }
      if (largestIsLast && !field.name().equals(largestField)) {
        largestIsLast = false;
      }
      if (field.numericValue() != null) { // just ignore these as non-competitive (avoid toString'ing their number)
        continue;
      }
      String strVal = field.stringValue();
      if (strVal != null) {
        if (strVal.length() > largestFieldLen) {
          largestField = field.name();
          largestFieldLen = strVal.length();
          largestIsLast = true;
        }
      } else {
        BytesRef bytesRef = field.binaryValue();
        if (bytesRef != null && bytesRef.length > largestFieldLen) {
          largestField = field.name();
          largestFieldLen = bytesRef.length;
          largestIsLast = true;
        }
      }
    }
    if (!largestIsLast && largestField != null && largestFieldLen > MIN_LENGTH_TO_MOVE_LAST) { // only bother if the value isn't tiny
      LinkedList<IndexableField> addToEnd = new LinkedList<>();
      Iterator<IndexableField> iterator = doc.iterator();
      while (iterator.hasNext()) {
        IndexableField field = iterator.next();
        if (field.name().equals(largestField)) {
          addToEnd.add(field);
          iterator.remove(); // Document may not have "remove" but it's iterator allows mutation
        }
      }
      for (IndexableField field : addToEnd) {
        doc.add(field);
      }
    }
  }
}
