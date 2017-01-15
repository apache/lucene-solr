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
  int overrequest = -1; // Number of buckets to request beyond the limit to do internally during distributed search. -1 means default.
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
    DV,  // DocValues, collect into ordinal array
    UIF, // UnInvertedField, collect into ordinal array
    DVHASH, // DocValues, collect into hash
    ENUM, // TermsEnum then intersect DocSet (stream-able)
    STREAM, // presently equivalent to ENUM
    SMART,
    ;

    public static FacetMethod fromString(String method) {
      if (method == null || method.length()==0) return DEFAULT_METHOD;
      switch (method) {
        case "dv": return DV;
        case "uif": return UIF;
        case "dvhash": return DVHASH;
        case "enum": return ENUM;
        case "stream": return STREAM; // TODO replace with enum?
        case "smart": return SMART;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown FacetField method " + method);
      }
    }

    static FacetMethod DEFAULT_METHOD = SMART; // non-final for tests to vary
  }

  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    SchemaField sf = fcontext.searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    LegacyNumericType ntype = ft.getNumericType();
    // ensure we can support the requested options for numeric faceting:
    if (ntype != null) {
      if (prefix != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Doesn't make sense to set facet prefix on a numeric field");
      }
      if (mincount == 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Numeric fields do not support facet mincount=0; try indexing as terms");
        // TODO if indexed=true then we could add support
      }
    }

    // TODO auto-pick ENUM/STREAM SOLR-9351 when index asc and DocSet cardinality is *not* much smaller than term cardinality
    if (method == FacetMethod.ENUM) {// at the moment these two are the same
      method = FacetMethod.STREAM;
    }
    if (method == FacetMethod.STREAM && sf.indexed() &&
        "index".equals(sortVariable) && sortDirection == SortDirection.asc) {
      return new FacetFieldProcessorByEnumTermsStream(fcontext, this, sf);
    }

    // TODO if method=UIF and not single-valued numerics then simply choose that now? TODO add FieldType.getDocValuesType()

    if (!multiToken) {
      if (mincount > 0 && prefix == null && (ntype != null || method == FacetMethod.DVHASH)) {
        // TODO can we auto-pick for strings when term cardinality is much greater than DocSet cardinality?
        //   or if we don't know cardinality but DocSet size is very small
        return new FacetFieldProcessorByHashDV(fcontext, this, sf);
      } else if (ntype == null) {
        // single valued string...
        return new FacetFieldProcessorByArrayDV(fcontext, this, sf);
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Couldn't pick facet algorithm for field " + sf);
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



