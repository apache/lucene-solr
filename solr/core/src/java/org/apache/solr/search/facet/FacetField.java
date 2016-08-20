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
package org.apache.solr.search.facet;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.legacy.LegacyNumericType;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;


public class FacetField extends FacetRequest {
  String field;
  long offset;
  long limit = 10;
  long mincount = 1;
  boolean missing;
  boolean allBuckets;   // show cumulative stats across all buckets (this can be different than non-bucketed stats across all docs because of multi-valued docs)
  boolean numBuckets;
  String prefix;
  String sortVariable;
  SortDirection sortDirection;
  FacetMethod method;
  int cacheDf;  // 0 means "default", -1 means "never cache"

  // experimental - force perSeg collection when using dv method, currently for testing purposes only.
  Boolean perSeg;

  // TODO: put this somewhere more generic?
  public enum SortDirection {
    asc(-1) ,
    desc(1);

    private final int multiplier;
    private SortDirection(int multiplier) {
      this.multiplier = multiplier;
    }

    // asc==-1, desc==1
    public int getMultiplier() {
      return multiplier;
    }
  }

  public enum FacetMethod {
    DV,  // DocValues
    UIF, // UnInvertedField
    ENUM,
    STREAM,
    SMART,
    ;

    public static FacetMethod fromString(String method) {
      if (method == null || method.length()==0) return null;
      if ("dv".equals(method)) {
        return DV;
      } else if ("uif".equals(method)) {
        return UIF;
      } else if ("enum".equals(method)) {
        return ENUM;
      } else if ("smart".equals(method)) {
        return SMART;
      } else if ("stream".equals(method)) {
        return STREAM;
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown FacetField method " + method);
    }
  }

  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    SchemaField sf = fcontext.searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    if (method == FacetMethod.ENUM && sf.indexed()) {
      throw new UnsupportedOperationException();
    } else if (method == FacetMethod.STREAM && sf.indexed()) {
      return new FacetFieldProcessorByEnumTermsStream(fcontext, this, sf);
    }

    LegacyNumericType ntype = ft.getNumericType();

    if (!multiToken) {
      if (ntype != null) {
        // single valued numeric (docvalues or fieldcache)
        return new FacetFieldProcessorByHashNumeric(fcontext, this, sf);
      } else {
        // single valued string...
        return new FacetFieldProcessorByArrayDV(fcontext, this, sf);
      }
    }

    // multi-valued after this point

    if (sf.hasDocValues() || method == FacetMethod.DV) {
      // single and multi-valued string docValues
      return new FacetFieldProcessorByArrayDV(fcontext, this, sf);
    }

    // Top-level multi-valued field cache (UIF)
    return new FacetFieldProcessorByArrayUIF(fcontext, this, sf);
  }

  @Override
  public FacetMerger createFacetMerger(Object prototype) {
    return new FacetFieldMerger(this);
  }
  
  @Override
  public Map<String, Object> getFacetDescription() {
    Map<String, Object> descr = new HashMap<>();
    descr.put("field", field);
    descr.put("limit", limit);
    return descr;
  }
}



