package org.apache.solr.handler.component;

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
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.DateMathParser;

/**
 * Process date facets
 *
 * @deprecated the whole date faceting feature is deprecated. Use range facets instead which can
 * already work with dates.
 */
@Deprecated
public class DateFacetProcessor extends SimpleFacets {
  public DateFacetProcessor(SolrQueryRequest req, DocSet docs, SolrParams params, ResponseBuilder rb) {
    super(req, docs, params, rb);
  }

  /**
   * @deprecated Use getFacetRangeCounts which is more generalized
   */
  @Deprecated
  public void getFacetDateCounts(String dateFacet, NamedList<Object> resOuter)
      throws IOException {

    final IndexSchema schema = searcher.getSchema();

    ParsedParams parsed = null;
    try {
      parsed = parseParams(FacetParams.FACET_DATE, dateFacet);
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
    }

    final SolrParams params = parsed.params;
    final SolrParams required = parsed.required;
    final String key = parsed.key;
    final String f = parsed.facetValue;

    final NamedList<Object> resInner = new SimpleOrderedMap<>();
    resOuter.add(key, resInner);
    final SchemaField sf = schema.getField(f);
    if (!(sf.getType() instanceof TrieDateField)) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "Can not date facet on a field which is not a TrieDateField: " + f);
    }
    final TrieDateField ft = (TrieDateField) sf.getType();
    final String startS
        = required.getFieldParam(f, FacetParams.FACET_DATE_START);
    final Date start;
    try {
      start = ft.parseMath(null, startS);
    } catch (SolrException e) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'start' is not a valid Date string: " + startS, e);
    }
    final String endS
        = required.getFieldParam(f, FacetParams.FACET_DATE_END);
    Date end; // not final, hardend may change this
    try {
      end = ft.parseMath(null, endS);
    } catch (SolrException e) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'end' is not a valid Date string: " + endS, e);
    }

    if (end.before(start)) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'end' comes before 'start': " + endS + " < " + startS);
    }

    final String gap = required.getFieldParam(f, FacetParams.FACET_DATE_GAP);
    final DateMathParser dmp = new DateMathParser();

    final int minCount = params.getFieldInt(f, FacetParams.FACET_MINCOUNT, 0);

    String[] iStrs = params.getFieldParams(f, FacetParams.FACET_DATE_INCLUDE);
    // Legacy support for default of [lower,upper,edge] for date faceting
    // this is not handled by FacetRangeInclude.parseParam because
    // range faceting has differnet defaults
    final EnumSet<FacetParams.FacetRangeInclude> include =
        (null == iStrs || 0 == iStrs.length) ?
            EnumSet.of(FacetParams.FacetRangeInclude.LOWER,
                FacetParams.FacetRangeInclude.UPPER,
                FacetParams.FacetRangeInclude.EDGE)
            : FacetParams.FacetRangeInclude.parseParam(iStrs);

    try {
      Date low = start;
      while (low.before(end)) {
        dmp.setNow(low);
        String label = ft.toExternal(low);

        Date high = dmp.parseMath(gap);
        if (end.before(high)) {
          if (params.getFieldBool(f, FacetParams.FACET_DATE_HARD_END, false)) {
            high = end;
          } else {
            end = high;
          }
        }
        if (high.before(low)) {
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "date facet infinite loop (is gap negative?)");
        }
        if (high.equals(low)) {
          throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
                  "date facet infinite loop: gap is effectively zero");
        }
        final boolean includeLower =
            (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                (include.contains(FacetParams.FacetRangeInclude.EDGE) && low.equals(start)));
        final boolean includeUpper =
            (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                (include.contains(FacetParams.FacetRangeInclude.EDGE) && high.equals(end)));

        final int count = rangeCount(parsed, sf, low, high, includeLower, includeUpper);
        if (count >= minCount) {
          resInner.add(label, count);
        }
        low = high;
      }
    } catch (java.text.ParseException e) {
      throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
              "date facet 'gap' is not a valid Date Math string: " + gap, e);
    }

    // explicitly return the gap and end so all the counts
    // (including before/after/between) are meaningful - even if mincount
    // has removed the neighboring ranges
    resInner.add("gap", gap);
    resInner.add("start", start);
    resInner.add("end", end);

    final String[] othersP =
        params.getFieldParams(f, FacetParams.FACET_DATE_OTHER);
    if (null != othersP && 0 < othersP.length) {
      final Set<FacetParams.FacetRangeOther> others = EnumSet.noneOf(FacetParams.FacetRangeOther.class);

      for (final String o : othersP) {
        others.add(FacetParams.FacetRangeOther.get(o));
      }

      // no matter what other values are listed, we don't do
      // anything if "none" is specified.
      if (!others.contains(FacetParams.FacetRangeOther.NONE)) {
        boolean all = others.contains(FacetParams.FacetRangeOther.ALL);

        if (all || others.contains(FacetParams.FacetRangeOther.BEFORE)) {
          // include upper bound if "outer" or if first gap doesn't already include it
          resInner.add(FacetParams.FacetRangeOther.BEFORE.toString(),
              rangeCount(parsed, sf, null, start,
                  false,
                  (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
                      (!(include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                          include.contains(FacetParams.FacetRangeInclude.EDGE))))));
        }
        if (all || others.contains(FacetParams.FacetRangeOther.AFTER)) {
          // include lower bound if "outer" or if last gap doesn't already include it
          resInner.add(FacetParams.FacetRangeOther.AFTER.toString(),
              rangeCount(parsed, sf, end, null,
                  (include.contains(FacetParams.FacetRangeInclude.OUTER) ||
                      (!(include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                          include.contains(FacetParams.FacetRangeInclude.EDGE)))),
                  false));
        }
        if (all || others.contains(FacetParams.FacetRangeOther.BETWEEN)) {
          resInner.add(FacetParams.FacetRangeOther.BETWEEN.toString(),
              rangeCount(parsed, sf, start, end,
                  (include.contains(FacetParams.FacetRangeInclude.LOWER) ||
                      include.contains(FacetParams.FacetRangeInclude.EDGE)),
                  (include.contains(FacetParams.FacetRangeInclude.UPPER) ||
                      include.contains(FacetParams.FacetRangeInclude.EDGE))));
        }
      }
    }
  }

  /**
   * Returns a list of value constraints and the associated facet counts
   * for each facet date field, range, and interval specified in the
   * SolrParams
   *
   * @see FacetParams#FACET_DATE
   * @deprecated Use getFacetRangeCounts which is more generalized
   */
  @Deprecated
  public NamedList<Object> getFacetDateCounts()
      throws IOException {

    final NamedList<Object> resOuter = new SimpleOrderedMap<>();
    final String[] fields = global.getParams(FacetParams.FACET_DATE);

    if (null == fields || 0 == fields.length) return resOuter;

    for (String f : fields) {
      getFacetDateCounts(f, resOuter);
    }

    return resOuter;
  }

  /**
   * @deprecated Use rangeCount(SchemaField,String,String,boolean,boolean) which is more generalized
   */
  @Deprecated
  protected int rangeCount(ParsedParams parsed, SchemaField sf, Date low, Date high,
                           boolean iLow, boolean iHigh) throws IOException {
    Query rangeQ = ((TrieDateField) (sf.getType())).getRangeQuery(null, sf, low, high, iLow, iHigh);
    return searcher.numDocs(rangeQ, parsed.docs);
  }
}

