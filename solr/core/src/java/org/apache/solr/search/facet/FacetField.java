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

import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;

public class FacetField extends FacetRequestSorted {
  public static final int DEFAULT_FACET_LIMIT = 10;
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
    limit = DEFAULT_FACET_LIMIT;
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
  @SuppressWarnings("rawtypes")
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    SchemaField sf = fcontext.searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    if (fcontext.facetInfo != null) {
      // refinement... we will end up either skipping the entire facet, or doing calculating only specific facet buckets
      if (multiToken && !sf.hasDocValues() && method!=FacetMethod.DV && sf.isUninvertible()) {
        // Match the access method from the first phase.
        // It won't always matter, but does currently for an all-values bucket
        return new FacetFieldProcessorByArrayUIF(fcontext, this, sf);
      }
      return new FacetFieldProcessorByArrayDV(fcontext, this, sf);
    }

    NumberType ntype = ft.getNumberType();
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
    if (method == FacetMethod.STREAM && sf.indexed() && !ft.isPointField() &&
        // streaming doesn't support allBuckets, numBuckets or missing
        // so, don't use stream processor if anyone of them is enabled
        !(allBuckets || numBuckets || missing) &&
        // whether we can use stream processing depends on whether this is a shard request, whether
        // re-sorting has been requested, and if the effective sort during collection is "index asc"
        ( fcontext.isShard()
          // for a shard request, the effective per-shard sort must be index asc
          ? FacetSort.INDEX_ASC.equals(null == prelim_sort ? sort : prelim_sort)
          // for a non-shard request, we can only use streaming if there is no pre-sorting
          : (null == prelim_sort && FacetSort.INDEX_ASC.equals( sort ) ) ) ) {
          
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

    if (sf.hasDocValues() && sf.getType().isPointField()) {
      return new FacetFieldProcessorByHashDV(fcontext, this, sf);
    }

    // multi-valued after this point

    if (sf.hasDocValues() || method == FacetMethod.DV || !sf.isUninvertible()) {
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



