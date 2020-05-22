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

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.search.SyntaxError;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

class FacetRangeParser extends FacetParser<FacetRange> {
  @SuppressWarnings({"rawtypes"})
  public FacetRangeParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetRange();
  }

  public FacetRange parse(Object arg) throws SyntaxError {
    parseCommonParams(arg);

    if (!(arg instanceof Map)) {
      throw err("Missing range facet arguments");
    }

    @SuppressWarnings({"unchecked"})
    Map<String, Object> m = (Map<String, Object>) arg;

    facet.field = getString(m, "field", null);
    facet.ranges = getVal(m, "ranges", false);

    boolean required = facet.ranges == null;
    facet.start = getVal(m, "start", required);
    facet.end = getVal(m, "end", required);
    facet.gap = getVal(m, "gap", required);
    facet.hardend = getBoolean(m, "hardend", facet.hardend);
    facet.mincount = getLong(m, "mincount", 0);

    // TODO: refactor list-of-options code

    List<String> list = getStringList(m, "include", false);
    String[] includeList = null;
    if (list != null) {
      includeList = list.toArray(new String[list.size()]);
    }
    facet.include = FacetParams.FacetRangeInclude.parseParam( includeList );
    facet.others = EnumSet.noneOf(FacetParams.FacetRangeOther.class);

    List<String> other = getStringList(m, "other", false);
    if (other != null) {
      for (String otherStr : other) {
        facet.others.add( FacetParams.FacetRangeOther.get(otherStr) );
      }
    }

    Object facetObj = m.get("facet");
    parseSubs(facetObj);

    return facet;
  }

}
