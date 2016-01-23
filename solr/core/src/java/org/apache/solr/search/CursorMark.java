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

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import static org.apache.solr.common.params.CursorMarkParams.*;

import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

/**
 * An object that encapsulates the basic information about the current Mark Point of a 
 * "Cursor" based request.  <code>CursorMark</code> objects track the sort values of 
 * the last document returned to a user, so that {@link SolrIndexSearcher} can then 
 * be asked to find all documents "after" the values represented by this 
 * <code>CursorMark</code>.
 *
 */
public final class CursorMark {

  /**
   * Used for validation and (un)marshalling of sort values
   */
  private final SortSpec sortSpec;

  /**
   * The raw, unmarshalled, sort values (that corrispond with the SortField's in the 
   * SortSpec) for knowing which docs this cursor should "search after".  If this 
   * list is null, then we have no specific values to "search after" and we 
   * should start from the very begining of the sorted list of documents matching 
   * the query.
   */
  private List<Object> values = null;

  /**
   * for serializing this CursorMark as a String
   */
  private final JavaBinCodec codec = new JavaBinCodec();

  /**
   * Generates an empty CursorMark bound for use with the 
   * specified schema and {@link SortSpec}.
   *
   * @param schema used for basic validation
   * @param sortSpec bound to this totem (un)marshalling serialized values
   */
  public CursorMark(IndexSchema schema, SortSpec sortSpec) {

    final SchemaField uniqueKey = schema.getUniqueKeyField();
    if (null == uniqueKey) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
                              "Cursor functionality is not available unless the IndexSchema defines a uniqueKey field");
    }

    final Sort sort = sortSpec.getSort();
    if (null == sort) {
      // pure score, by definition we don't include the mandatyr uniqueKey tie breaker
      throw new SolrException(ErrorCode.BAD_REQUEST,
                              "Cursor functionality requires a sort containing a uniqueKey field tie breaker");
    }
    
    if (!sortSpec.getSchemaFields().contains(uniqueKey)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
                              "Cursor functionality requires a sort containing a uniqueKey field tie breaker");
    }

    if (0 != sortSpec.getOffset()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
                              "Cursor functionality requires start=0");
    }

    for (SortField sf : sort.getSort()) {
      if (sf.getType().equals(SortField.Type.DOC)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
                                "Cursor functionality can not be used with internal doc ordering sort: _docid_");
      }
    }

    if (sort.getSort().length != sortSpec.getSchemaFields().size()) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
                                "Cursor SortSpec failure: sort length != SchemaFields: " 
                                + sort.getSort().length + " != " + 
                                sortSpec.getSchemaFields().size());
    }

    this.sortSpec = sortSpec;
    this.values = null;
  }

  /**
   * Generates an empty CursorMark bound for use with the same {@link SortSpec}
   * as the specified existing CursorMark.
   *
   * @param previous Existing CursorMark whose SortSpec will be reused in the new CursorMark.
   * @see #createNext
   */
  private CursorMark(CursorMark previous) {
    this.sortSpec = previous.sortSpec;
    this.values = null;
  }

  /**
   * Generates an new CursorMark bound for use with the same {@link SortSpec}
   * as the current CursorMark but using the new SortValues.
   *
   */
  public CursorMark createNext(List<Object> nextSortValues) {
    final CursorMark next = new CursorMark(this);
    next.setSortValues(nextSortValues);
    return next;
  }


  /**
   * Sets the (raw, unmarshalled) sort values (which must conform to the existing 
   * sortSpec) to populate this object.  If null, then there is nothing to 
   * "search after" and the "first page" of results should be returned.
   */
  public void setSortValues(List<Object> input) {
    if (null == input) {
      this.values = null;
    } else {
      assert input.size() == sortSpec.getSort().getSort().length;
      // defensive copy
      this.values = new ArrayList<>(input);
    }
  }

  /**
   * Returns a copy of the (raw, unmarshalled) sort values used by this object, or 
   * null if first page of docs should be returned (ie: no sort after)
   */
  public List<Object> getSortValues() {
    // defensive copy
    return null == this.values ? null : new ArrayList<>(this.values);
  }

  /**
   * Returns the SortSpec used by this object.
   */
  public SortSpec getSortSpec() {
    return this.sortSpec;
  }

  /**
   * Parses the serialized version of a CursorMark from a client 
   * (which must conform to the existing sortSpec) and populates this object.
   *
   * @see #getSerializedTotem
   */
  public void parseSerializedTotem(final String serialized) {
    if (CURSOR_MARK_START.equals(serialized)) {
      values = null;
      return;
    }
    final SortField[] sortFields = sortSpec.getSort().getSort();
    final List<SchemaField> schemaFields = sortSpec.getSchemaFields();

    List<Object> pieces = null;
    try {
      final byte[] rawData = Base64.base64ToByteArray(serialized);
      ByteArrayInputStream in = new ByteArrayInputStream(rawData);
      try {
        pieces = (List<Object>) codec.unmarshal(in);
        boolean b = false;
        for (Object o : pieces) {
          if (o instanceof BytesRefBuilder || o instanceof BytesRef || o instanceof String) {
            b = true; break;
          }
        }
        if (b) {
          in.reset();
          pieces = (List<Object>) codec.unmarshal(in);
        }
      } finally {
        in.close();
      }
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
                              "Unable to parse '"+CURSOR_MARK_PARAM+"' after totem: " + 
                              "value must either be '"+CURSOR_MARK_START+"' or the " + 
                              "'"+CURSOR_MARK_NEXT+"' returned by a previous search: "
                              + serialized, ex);
    }
    assert null != pieces : "pieces wasn't parsed?";

    if (sortFields.length != pieces.size()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
                              CURSOR_MARK_PARAM+" does not work with current sort (wrong size): " + serialized);
    }


    this.values = new ArrayList<>(sortFields.length);

    final BytesRef tmpBytes = new BytesRef();
    for (int i = 0; i < sortFields.length; i++) {

      SortField curSort = sortFields[i];
      SchemaField curField = schemaFields.get(i);
      Object rawValue = pieces.get(i);

      if (null != curField) {
        FieldType curType = curField.getType();
        rawValue = curType.unmarshalSortValue(rawValue);
      } 

      this.values.add(rawValue);
    }
  }
  
  /**
   * Generates a Base64 encoded serialized representation of the sort values 
   * encapsulated by this object, for use in cursor requests.
   *
   * @see #parseSerializedTotem
   */
  public String getSerializedTotem() {
    if (null == this.values) {
      return CURSOR_MARK_START;
    }

    final List<SchemaField> schemaFields = sortSpec.getSchemaFields();
    final ArrayList<Object> marshalledValues = new ArrayList<>(values.size()+1);
    for (int i = 0; i < schemaFields.size(); i++) {
      SchemaField fld = schemaFields.get(i);
      Object safeValue = values.get(i);
      if (null != fld) {
        FieldType type = fld.getType();
        safeValue = type.marshalSortValue(safeValue);
      }
      marshalledValues.add(safeValue);
    }

    // TODO: we could also encode info about the SortSpec for error checking:
    // the type/name/dir from the SortFields (or a hashCode to act as a checksum) 
    // could help provide more validation beyond just the number of clauses.

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(256);
      try {
        codec.marshal(marshalledValues, out);
        byte[] rawData = out.toByteArray();
        return Base64.byteArrayToBase64(rawData, 0, rawData.length);
      } finally {
        out.close();
      }
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
                              "Unable to format search after totem", ex);
      
    }
  }

  /**
   * Returns a synthetically constructed {@link FieldDoc} whose {@link FieldDoc#fields} 
   * match the values of this object.  
   * <p>
   * Important Notes:
   * </p>
   * <ul>
   *  <li>{@link FieldDoc#doc} will always be set to {@link Integer#MAX_VALUE} so 
   *    that the tie breaking logic used by <code>IndexSearcher</code> won't select 
   *    the same doc again based on the internal lucene docId when the Solr 
   *    <code>uniqueKey</code> value is the same.
   *  </li>
   *  <li>{@link FieldDoc#score} will always be set to 0.0F since it is not used
   *    when applying <code>searchAfter</code> logic. (Even if the sort values themselves 
   *    contain scores which are used in the sort)
   *  </li>
   * </ul>
   *
   * @return a {@link FieldDoc} to "search after" or null if the initial 
   *         page of results is requested.
   */
  public FieldDoc getSearchAfterFieldDoc() {
    if (null == values) return null;

    return new FieldDoc(Integer.MAX_VALUE, 0.0F, values.toArray());
  }

}
