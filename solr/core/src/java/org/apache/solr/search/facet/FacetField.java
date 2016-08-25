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

// Any type of facet request that generates a variable number of buckets
// and the ability to sort by those generated buckets.
abstract class FacetRequestSorted extends FacetRequest {
  long offset;
  long limit;
  long mincount;
  String sortVariable;
  SortDirection sortDirection;
  RefineMethod refine; // null, NONE, or SIMPLE

  @Override
  public RefineMethod getRefineMethod() {
    return refine;
  }

  @Override
  public boolean returnsPartial() {
    return limit > 0;
  }

}


public class FacetField extends FacetRequestSorted {
  String field;
  boolean missing;
  boolean allBuckets;   // show cumulative stats across all buckets (this can be different than non-bucketed stats across all docs because of multi-valued docs)
  boolean numBuckets;
  String prefix;
  FacetMethod method;
  int cacheDf;  // 0 means "default", -1 means "never cache"

  // experimental - force perSeg collection when using dv method, currently for testing purposes only.
  Boolean perSeg;

  {
    // defaults for FacetRequestSorted
    mincount = 1;
    limit = 10;
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



