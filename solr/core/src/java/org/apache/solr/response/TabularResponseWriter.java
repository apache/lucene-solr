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

package org.apache.solr.response;


import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;

/**
 * Base response writer for table-oriented data
 */
public abstract class TabularResponseWriter extends TextResponseWriter {

  private boolean returnStoredOrDocValStored;

  public TabularResponseWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse resp) {
    super(writer, req, resp);
    // fl=* or globs specified in fl
    returnStoredOrDocValStored = ((returnFields.getRequestedFieldNames() == null) ||
        returnFields.hasPatternMatching());
  }

  /**
   * Returns fields to be returned in the response
   */
  public Collection<String> getFields() {
    Collection<String> fields = returnFields.getRequestedFieldNames();
    Set<String> explicitReqFields = returnFields.getExplicitlyRequestedFieldNames();
    Object responseObj = rsp.getResponse();
    if (fields==null||returnFields.hasPatternMatching()) {
      if (responseObj instanceof SolrDocumentList) {
        // get the list of fields from the SolrDocumentList
        if(fields==null) {
          fields = new LinkedHashSet<>();
        }
        for (SolrDocument sdoc: (SolrDocumentList)responseObj) {
          fields.addAll(sdoc.getFieldNames());
        }
      } else {
        // get the list of fields from the index
        Iterable<String> all = req.getSearcher().getFieldNames();
        if (fields == null) {
          fields = Sets.newHashSet(all);
        } else {
          Iterables.addAll(fields, all);
        }
      }

      if (explicitReqFields != null) {
        // add explicit requested fields
        Iterables.addAll(fields, explicitReqFields);
      }

      if (returnFields.wantsScore()) {
        fields.add("score");
      } else {
        fields.remove("score");
      }
    }
    return fields;
  }

  /**
   * Returns true if field needs to be skipped else false
   * @param field name of the field
   * @return boolean value
   */
  public boolean shouldSkipField(String field) {
    Set<String> explicitReqFields = returnFields.getExplicitlyRequestedFieldNames();
    SchemaField sf = schema.getFieldOrNull(field);

    // Return stored fields or useDocValuesAsStored=true fields,
    // unless an explicit field list is specified
    return  (returnStoredOrDocValStored && !(explicitReqFields != null && explicitReqFields.contains(field)) &&
        sf!= null && !sf.stored() && !(sf.hasDocValues() && sf.useDocValuesAsStored()));
  }

  public void writeResponse(Object responseObj) throws IOException {
    if (responseObj instanceof ResultContext) {
      writeDocuments(null, (ResultContext)responseObj );
    }
    else if (responseObj instanceof DocList) {
      ResultContext ctx = new BasicResultContext((DocList)responseObj, returnFields, null, null, req);
      writeDocuments(null, ctx );
    } else if (responseObj instanceof SolrDocumentList) {
      writeSolrDocumentList(null, (SolrDocumentList)responseObj, returnFields );
    }
  }

  @Override
  public void writeNamedList(String name, @SuppressWarnings({"rawtypes"})NamedList val) throws IOException {
  }

  @Override
  public void writeStartDocumentList(String name,
                                     long start, int size, long numFound, Float maxScore) throws IOException
  {
    // nothing
  }
  
  @Override
  public void writeStartDocumentList(String name,
                                     long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException
  {
    // nothing
  }

  @Override
  public void writeEndDocumentList() throws IOException
  {
    // nothing
  }

  @Override
  public void writeMap(String name, @SuppressWarnings({"rawtypes"})Map val, boolean excludeOuter, boolean isFirstVal) throws IOException {
  }

  @Override
  public void writeArray(String name, @SuppressWarnings({"rawtypes"})Iterator val) throws IOException {
  }

  @Override
  public void writeDate(String name, Date val) throws IOException {
    writeDate(name, val.toInstant().toString());
  }
}
