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
package org.apache.solr.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/** 
 * <p>
 * <code>SortableTextField</code> is a specialized form of {@link TextField} that supports 
 * Sorting and ValueSource functions, using <code>docValues</code> built from the first 
 * <code>maxCharsForDocValues</code> characters of the original (pre-analyzed) String values of this field.
 * </p>
 * <p>
 * The implicit default value for <code>maxCharsForDocValues</code> is <code>1024</code>.  If a field 
 * type instance is configured with <code>maxCharsForDocValues &lt;= 0</code> this overrides the default 
 * with an effective value of "no limit" ({@link Integer#MAX_VALUE}).
 * </p>
 * <p>
 * Instances of this FieldType implicitly default to <code>docValues="true"</code> unless explicitly 
 * configured with <code>docValues="false"</code>.
 * </p>
 * <p>
 * Just like {@link StrField}, instances of this field that are <code>multiValued="true"</code> support 
 * the <code>field(name,min|max)</code> function, and implicitly sort on <code>min|max</code> depending 
 * on the <code>asc|desc</code> direction selector.
 * </p>
 *
 * <p>
 * <b>NOTE:</b> Unlike most other FieldTypes, this class defaults to 
 * <code>useDocValuesAsStored="false"</code>.  If an instance of this type (or a field that uses this type) 
 * overrides this behavior to set <code>useDocValuesAsStored="true"</code> then instead of truncating the 
 * original string value based on the effective value of <code>maxCharsForDocValues</code>, this class 
 * will reject any documents w/a field value longer then that limit -- causing the document update to fail.
 * This behavior exists to prevent situations that could result in a search client reieving only a truncated
 * version of the original field value in place of a <code>stored</code> value.
 * </p>
 */
public class SortableTextField extends TextField {

  public static final int DEFAULT_MAX_CHARS_FOR_DOC_VALUES = 1024;
  
  private int maxCharsForDocValues = DEFAULT_MAX_CHARS_FOR_DOC_VALUES;
  
  protected void init(IndexSchema schema, Map<String,String> args) {
    { 
      final String maxS = args.remove("maxCharsForDocValues");
      if (maxS != null) {
        maxCharsForDocValues = Integer.parseInt(maxS);
        if (maxCharsForDocValues <= 0) {
          maxCharsForDocValues = Integer.MAX_VALUE;
        }
      }
    }

    // by the time our init() is called, super.setArgs has already removed & processed any explicit
    // "docValues=foo" or useDocValuesAsStored=bar args...
    //  - If the user explicitly said docValues=false, we want to respect that and not change it.
    //    - if the user didn't explicitly specify anything, then we want to implicitly *default* docValues=true
    //  - The inverse is true for useDocValuesAsStored=true:
    //    - if explict, then respect it; else implicitly default to useDocValuesAsStored=false
    // ...lucky for us, setArgs preserved info about explicitly set true|false properties...
    if (! on(falseProperties, DOC_VALUES)) {
      properties |= DOC_VALUES;
    }
    if (! on(trueProperties, USE_DOCVALUES_AS_STORED)) {
      properties &= ~USE_DOCVALUES_AS_STORED;
    }
    
    super.init(schema, args);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    IndexableField f = createField( field, value);
    if (! field.hasDocValues()) {
      return Collections.singletonList(f);
    }
    if (value instanceof ByteArrayUtf8CharSequence) {
      ByteArrayUtf8CharSequence utf8 = (ByteArrayUtf8CharSequence) value;
      if (utf8.size() < maxCharsForDocValues) {
        BytesRef bytes = new BytesRef(utf8.getBuf(), utf8.offset(), utf8.size());
        return getIndexableFields(field, f, bytes);
      }
    }
    final String origString = value.toString();
    final int origLegth = origString.length();
    final boolean truncate = maxCharsForDocValues < origLegth;
    if (field.useDocValuesAsStored() && truncate) {
      // if the user has explicitly configured useDocValuesAsStored, we need a special
      // check to fail docs where the values are too long -- we don't want to silently
      // accept and then have search queries returning partial values
      throw new SolrException
        (SolrException.ErrorCode.BAD_REQUEST,
         "Can not use field " + field.getName() + " with values longer then maxCharsForDocValues=" +
         maxCharsForDocValues + " when useDocValuesAsStored=true (length=" + origLegth + ")");
    }
    final BytesRef bytes = new BytesRef(truncate ? origString.subSequence(0, maxCharsForDocValues) : origString);

    return getIndexableFields(field, f, bytes);
  }

  private static List<IndexableField> getIndexableFields(SchemaField field, IndexableField f, BytesRef bytes) {
    final IndexableField docval = field.multiValued()
      ? new SortedSetDocValuesField(field.getName(), bytes)
      : new SortedDocValuesField(field.getName(), bytes);
    
    if (null == f) {
      return Collections.singletonList(docval);
    } 
    return Arrays.asList(f, docval);
  }

  
  /** 
   * {@inheritDoc} 
   * this field type supports DocValues, this method is always a No-Op 
   */
  @Override
  protected void checkSupportsDocValues() {
    // No-Op
  }
  
  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    if (! field.hasDocValues()) {
      // type defaults to docValues=true, so error msg from perspective that
      // either type or field must have docValues="false"
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "Can not sort on this type of field when docValues=\"false\", field: " + field.getName());
    }
    
    // NOTE: we explicitly bypass super.getSortField so that our getDefaultMultiValueSelectorForSort
    // is used and we don't get the historic Uninversion behavior of TextField.
    return getStringSort(field, reverse);
  }
  
  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    if (! field.hasDocValues()) {
      // type defaults to docValues=true, so error msg from perspective that
      // either type or field must have docValues="false"
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "Can not use ValueSource on this type of field when docValues=\"false\", field: " + field.getName());
    }
    return super.getValueSource(field, parser);
  }
  
  @Override
  public MultiValueSelector getDefaultMultiValueSelectorForSort(SchemaField field, boolean reverse) {
    return reverse ? MultiValueSelector.MAX : MultiValueSelector.MIN;
  }
  
  @Override
  public ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    // trivial base case
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, parser);
    }
    
    // See LUCENE-6709
    if (! field.hasDocValues()) {
      // type defaults to docValues=true, so error msg from perspective that
      // either type or field must have docValues="false"
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "Can not select  '" + choice.toString() + "' value from multivalued field ("+
                              field.getName() +") when docValues=\"false\", field: " + field.getName());
    }
    SortedSetSelector.Type selectorType = choice.getSortedSetSelectorType();
    if (null == selectorType) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              choice.toString() + " is not a supported option for picking a single value"
                              + " from the multivalued field: " + field.getName() +
                              " (type: " + this.getTypeName() + ")");
    }
    
    return new SortedSetFieldSource(field.getName(), selectorType);
  }

  /** 
   * {@inheritDoc} 
   * this field type is not uninvertable, this method always returns null 
   */
  @Override
  public Type getUninversionType(SchemaField sf) {
    return null;
  }

  /** 
   * {@inheritDoc} 
   * This implementation always returns false. 
   */
  @Override
  public boolean multiValuedFieldCache() {
    return false;
  }

}
