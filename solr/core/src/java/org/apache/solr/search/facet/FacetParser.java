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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import static org.apache.solr.common.params.CommonParams.SORT;

abstract class FacetParser<FacetRequestT extends FacetRequest> {
  protected FacetRequestT facet;
  protected FacetParser<?> parent;
  protected String key;

  public FacetParser(FacetParser<?> parent, String key) {
    this.parent = parent;
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public String getPathStr() {
    if (parent == null) {
      return "/" + key;
    }
    return parent.getKey() + "/" + key;
  }

  protected RuntimeException err(String msg) {
    return new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg + " , path="+getPathStr());
  }

  public abstract FacetRequest parse(Object o) throws SyntaxError;

  // TODO: put the FacetRequest on the parser object?
  public void parseSubs(Object o) throws SyntaxError {
    if (o==null) return;
    if (o instanceof Map) {
      @SuppressWarnings({"unchecked"})
      Map<String,Object> m = (Map<String, Object>) o;
      for (Map.Entry<String,Object> entry : m.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        if ("processEmpty".equals(key)) {
          facet.processEmpty = getBoolean(m, "processEmpty", false);
          continue;
        }

        // "my_prices" : { "range" : { "field":...
        // key="my_prices", value={"range":..

        Object parsedValue = parseFacetOrStat(key, value);

        // TODO: have parseFacetOrStat directly add instead of return?
        if (parsedValue instanceof FacetRequest) {
          facet.addSubFacet(key, (FacetRequest)parsedValue);
        } else if (parsedValue instanceof AggValueSource) {
          facet.addStat(key, (AggValueSource)parsedValue);
        } else {
          throw err("Unknown facet type key=" + key + " class=" + (parsedValue == null ? "null" : parsedValue.getClass().getName()));
        }
      }
    } else {
      // facet : my_field?
      throw err("Expected map for facet/stat");
    }
  }

  public Object parseFacetOrStat(String key, Object o) throws SyntaxError {

    if (o instanceof String) {
      return parseStringFacetOrStat(key, (String)o);
    }

    if (!(o instanceof Map)) {
      throw err("expected Map but got " + o);
    }

    // The type can be in a one element map, or inside the args as the "type" field
    // { "query" : "foo:bar" }
    // { "range" : { "field":... } }
    // { "type"  : range, field : myfield, ... }
    @SuppressWarnings({"unchecked"})
    Map<String,Object> m = (Map<String,Object>)o;
    String type;
    Object args;

    if (m.size() == 1) {
      Map.Entry<String,Object> entry = m.entrySet().iterator().next();
      type = entry.getKey();
      args = entry.getValue();
      // throw err("expected facet/stat type name, like {range:{... but got " + m);
    } else {
      // type should be inside the map as a parameter
      Object typeObj = m.get("type");
      if (!(typeObj instanceof String)) {
        throw err("expected facet/stat type name, like {type:range, field:price, ...} but got " + typeObj);
      }
      type = (String)typeObj;
      args = m;
    }

    return parseFacetOrStat(key, type, args);
  }

  public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
    // TODO: a place to register all these facet types?

    switch (type) {
      case "field":
      case "terms":
        return new FacetFieldParser(this, key).parse(args);
      case "query":
        return new FacetQueryParser(this, key).parse(args);
      case "range":
        return new FacetRangeParser(this, key).parse(args);
      case "heatmap":
        return new FacetHeatmap.Parser(this, key).parse(args);
      case "func":
        return parseStat(key, args);
    }

    throw err("Unknown facet or stat. key=" + key + " type=" + type + " args=" + args);
  }

  public Object parseStringFacetOrStat(String key, String s) throws SyntaxError {
    // "avg(myfield)"
    return parseStat(key, s);
    // TODO - simple string representation of facets
  }

  /** Parses simple strings like "avg(x)" in the context of optional local params (may be null) */
  private AggValueSource parseStatWithParams(String key, SolrParams localparams, String stat) throws SyntaxError {
    SolrQueryRequest req = getSolrRequest();
    FunctionQParser parser = new FunctionQParser(stat, localparams, req.getParams(), req);
    AggValueSource agg = parser.parseAgg(FunctionQParser.FLAG_DEFAULT);
    return agg;
  }

  /** Parses simple strings like "avg(x)" or robust Maps that may contain local params */
  private AggValueSource parseStat(String key, Object args) throws SyntaxError {
    assert null != args;

    if (args instanceof CharSequence) {
      // Both of these variants are already unpacked for us in this case, and use no local params...
      // 1) x:{func:'min(foo)'}
      // 2) x:'min(foo)'
      return parseStatWithParams(key, null, args.toString());
    }

    if (args instanceof Map) {
      @SuppressWarnings({"unchecked"})
      final Map<String,Object> statMap = (Map<String,Object>)args;
      return parseStatWithParams(key, jsonToSolrParams(statMap), statMap.get("func").toString());
    }

    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Stats must be specified as either a simple string, or a json Map");

  }


  private FacetRequest.Domain getDomain() {
    if (facet.domain == null) {
      facet.domain = new FacetRequest.Domain();
    }
    return facet.domain;
  }

  protected void parseCommonParams(Object o) {
    if (o instanceof Map) {
      @SuppressWarnings({"unchecked"})
      Map<String,Object> m = (Map<String,Object>)o;
      List<String> excludeTags = getStringList(m, "excludeTags");
      if (excludeTags != null) {
        getDomain().excludeTags = excludeTags;
      }

      Object domainObj =  m.get("domain");
      if (domainObj instanceof Map) {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> domainMap = (Map<String, Object>)domainObj;
        FacetRequest.Domain domain = getDomain();

        excludeTags = getStringList(domainMap, "excludeTags");
        if (excludeTags != null) {
          domain.excludeTags = excludeTags;
        }

        if (domainMap.containsKey("query")) {
          domain.explicitQueries = parseJSONQueryStruct(domainMap.get("query"));
          if (null == domain.explicitQueries) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'query' domain can not be null or empty");
          } else if (null != domain.excludeTags) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'query' domain can not be combined with 'excludeTags'");
          }
        }

        String blockParent = getString(domainMap, "blockParent", null);
        String blockChildren = getString(domainMap, "blockChildren", null);

        if (blockParent != null) {
          domain.toParent = true;
          domain.parents = blockParent;
        } else if (blockChildren != null) {
          domain.toChildren = true;
          domain.parents = blockChildren;
        }

        FacetRequest.Domain.JoinField.createJoinField(domain, domainMap);
        FacetRequest.Domain.GraphField.createGraphField(domain, domainMap);

        Object filterOrList = domainMap.get("filter");
        if (filterOrList != null) {
          assert domain.filters == null;
          domain.filters = parseJSONQueryStruct(filterOrList);
        }

      } else if (domainObj != null) {
        throw err("Expected Map for 'domain', received " + domainObj.getClass().getSimpleName() + "=" + domainObj);
      }
    }
  }

  /** returns null on null input, otherwise returns a list of the JSON query structures -- either
   * directly from the raw (list) input, or if raw input is a not a list then it encapsulates
   * it in a new list.
   */
  @SuppressWarnings({"unchecked"})
  private List<Object> parseJSONQueryStruct(Object raw) {
    List<Object> result = null;
    if (null == raw) {
      return result;
    } else if (raw instanceof List) {
      result = (List<Object>) raw;
    } else {
      result = new ArrayList<>(1);
      result.add(raw);
    }
    return result;
  }

  public String getField(Map<String,Object> args) {
    Object fieldName = args.get("field"); // TODO: pull out into defined constant
    if (fieldName == null) {
      fieldName = args.get("f");  // short form
    }
    if (fieldName == null) {
      throw err("Missing 'field'");
    }

    if (!(fieldName instanceof String)) {
      throw err("Expected string for 'field', got" + fieldName);
    }

    return (String)fieldName;
  }


  public Long getLongOrNull(Map<String,Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null) {
      if (required) {
        throw err("Missing required parameter '" + paramName + "'");
      }
      return null;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o);
    }

    return ((Number)o).longValue();
  }

  public long getLong(Map<String,Object> args, String paramName, long defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return ((Number)o).longValue();
  }

  public Double getDoubleOrNull(Map<String,Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null) {
      if (required) {
        throw err("Missing required parameter '" + paramName + "'");
      }
      return null;
    }
    if (!(o instanceof Number)) {
      throw err("Expected double type for param '" + paramName + "' but got " + o);
    }

    return ((Number)o).doubleValue();
  }

  public boolean getBoolean(Map<String,Object> args, String paramName, boolean defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    // TODO: should we be more flexible and accept things like "true" (strings)?
    // Perhaps wait until the use case comes up.
    if (!(o instanceof Boolean)) {
      throw err("Expected boolean type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (Boolean)o;
  }

  public Boolean getBooleanOrNull(Map<String, Object> args, String paramName) {
    Object o = args.get(paramName);

    if (o != null && !(o instanceof Boolean)) {
      throw err("Expected boolean type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }
    return (Boolean) o;
  }


  public String getString(Map<String,Object> args, String paramName, String defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof String)) {
      throw err("Expected string type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (String)o;
  }

  public Object getVal(Map<String, Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null && required) {
      throw err("Missing required parameter: '" + paramName + "'");
    }
    return o;
  }

  public List<String> getStringList(Map<String,Object> args, String paramName) {
    return getStringList(args, paramName, true);
  }

@SuppressWarnings({"unchecked"})
  public List<String> getStringList(Map<String, Object> args, String paramName, boolean decode) {
    Object o = args.get(paramName);
    if (o == null) {
      return null;
    }
    if (o instanceof List) {
      return (List<String>)o;
    }
    if (o instanceof String) {
      return StrUtils.splitSmart((String)o, ",", decode).stream()
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());
    }

    throw err("Expected list of string or comma separated string values for '" + paramName +
        "', received " + o.getClass().getSimpleName() + "=" + o);
  }

  public IndexSchema getSchema() {
    return parent.getSchema();
  }

  public SolrQueryRequest getSolrRequest() {
    return parent.getSolrRequest();
  }

  /**
   * Helper that handles the possibility of map values being lists
   * NOTE: does *NOT* fail on map values that are sub-maps (ie: nested json objects)
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SolrParams jsonToSolrParams(Map jsonObject) {
    // HACK, but NamedList already handles the list processing for us...
    NamedList<String> nl = new NamedList<>();
    nl.addAll(jsonObject);
    return SolrParams.toSolrParams(nl);
  }

  // TODO Make this private (or at least not static) and introduce
  // a newInstance method on FacetParser that returns one of these?
  static class FacetTopParser extends FacetParser<FacetQuery> {
    private SolrQueryRequest req;

    public FacetTopParser(SolrQueryRequest req) {
      super(null, "facet");
      this.facet = new FacetQuery();
      this.req = req;
    }

    @Override
    public FacetQuery parse(Object args) throws SyntaxError {
      parseSubs(args);
      return facet;
    }

    @Override
    public SolrQueryRequest getSolrRequest() {
      return req;
    }

    @Override
    public IndexSchema getSchema() {
      return req.getSchema();
    }
  }

  static class FacetQueryParser extends FacetParser<FacetQuery> {
    public FacetQueryParser(@SuppressWarnings("rawtypes") FacetParser parent, String key) {
      super(parent, key);
      facet = new FacetQuery();
    }

    @Override
    public FacetQuery parse(Object arg) throws SyntaxError {
      parseCommonParams(arg);

      String qstring = null;
      if (arg instanceof String) {
        // just the field name...
        qstring = (String)arg;

      } else if (arg instanceof Map) {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> m = (Map<String, Object>) arg;
        qstring = getString(m, "q", null);
        if (qstring == null) {
          qstring = getString(m, "query", null);
        }

        // OK to parse subs before we have parsed our own query?
        // as long as subs don't need to know about it.
        parseSubs( m.get("facet") );
      } else if (arg != null) {
        // something lke json.facet.facet.query=2
        throw err("Expected string/map for facet query, received " + arg.getClass().getSimpleName() + "=" + arg);
      }

      // TODO: substats that are from defaults!!!

      if (qstring != null) {
        QParser parser = QParser.getParser(qstring, getSolrRequest());
        parser.setIsFilter(true);
        facet.q = parser.getQuery();
      }

      return facet;
    }
  }

  /*** not a separate type of parser for now...
   static class FacetBlockParentParser extends FacetParser<FacetBlockParent> {
   public FacetBlockParentParser(FacetParser parent, String key) {
   super(parent, key);
   facet = new FacetBlockParent();
   }

   @Override
   public FacetBlockParent parse(Object arg) throws SyntaxError {
   parseCommonParams(arg);

   if (arg instanceof String) {
   // just the field name...
   facet.parents = (String)arg;

   } else if (arg instanceof Map) {
   Map<String, Object> m = (Map<String, Object>) arg;
   facet.parents = getString(m, "parents", null);

   parseSubs( m.get("facet") );
   }

   return facet;
   }
   }
   ***/

  static class FacetFieldParser extends FacetParser<FacetField> {
    @SuppressWarnings({"rawtypes"})
    public FacetFieldParser(FacetParser parent, String key) {
      super(parent, key);
      facet = new FacetField();
    }

    public FacetField parse(Object arg) throws SyntaxError {
      parseCommonParams(arg);
      if (arg instanceof String) {
        // just the field name...
        facet.field = (String)arg;

      } else if (arg instanceof Map) {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> m = (Map<String, Object>) arg;
        facet.field = getField(m);
        facet.offset = getLong(m, "offset", facet.offset);
        facet.limit = getLong(m, "limit", facet.limit);
        facet.overrequest = (int) getLong(m, "overrequest", facet.overrequest);
        facet.overrefine = (int) getLong(m, "overrefine", facet.overrefine);
        if (facet.limit == 0) facet.offset = 0;  // normalize.  an offset with a limit of non-zero isn't useful.
        facet.mincount = getLong(m, "mincount", facet.mincount);
        facet.missing = getBoolean(m, "missing", facet.missing);
        facet.numBuckets = getBoolean(m, "numBuckets", facet.numBuckets);
        facet.prefix = getString(m, "prefix", facet.prefix);
        facet.allBuckets = getBoolean(m, "allBuckets", facet.allBuckets);
        facet.method = FacetField.FacetMethod.fromString(getString(m, "method", null));
        facet.cacheDf = (int)getLong(m, "cacheDf", facet.cacheDf);

        // TODO: pull up to higher level?
        facet.refine = FacetRequest.RefineMethod.fromObj(m.get("refine"));

        facet.perSeg = getBooleanOrNull(m, "perSeg");

        // facet.sort may depend on a facet stat...
        // should we be parsing / validating this here, or in the execution environment?
        Object o = m.get("facet");
        parseSubs(o);

        facet.sort = parseAndValidateSort(facet, m, SORT);
        facet.prelim_sort = parseAndValidateSort(facet, m, "prelim_sort");
      } else if (arg != null) {
        // something like json.facet.facet.field=2
        throw err("Expected string/map for facet field, received " + arg.getClass().getSimpleName() + "=" + arg);
      }

      if (null == facet.sort) {
        facet.sort = FacetRequest.FacetSort.COUNT_DESC;
      }

      return facet;
    }

    /**
     * Parses, validates and returns the {@link FacetRequest.FacetSort} for given sortParam
     * and facet field
     * <p>
     *   Currently, supported sort specifications are 'mystat desc' OR {mystat: 'desc'}
     *   index - This is equivalent to 'index asc'
     *   count - This is equivalent to 'count desc'
     * </p>
     *
     * @param facet {@link FacetField} for which sort needs to be parsed and validated
     * @param args map containing the sortVal for given sortParam
     * @param sortParam parameter for which sort needs to parsed and validated
     * @return parsed facet sort
     */
    private static FacetRequest.FacetSort parseAndValidateSort(FacetField facet, Map<String, Object> args, String sortParam) {
      Object sort = args.get(sortParam);
      if (sort == null) {
        return null;
      }

      FacetRequest.FacetSort facetSort = null;

      if (sort instanceof String) {
        String sortStr = (String)sort;
        if (sortStr.endsWith(" asc")) {
          facetSort =  new FacetRequest.FacetSort(sortStr.substring(0, sortStr.length()-" asc".length()),
                  FacetRequest.SortDirection.asc);
        } else if (sortStr.endsWith(" desc")) {
          facetSort =  new FacetRequest.FacetSort(sortStr.substring(0, sortStr.length()-" desc".length()),
                  FacetRequest.SortDirection.desc);
        } else {
          facetSort =  new FacetRequest.FacetSort(sortStr,
                  // default direction for "index" is ascending
                  ("index".equals(sortStr)
                          ? FacetRequest.SortDirection.asc
                          : FacetRequest.SortDirection.desc));
        }
      } else if (sort instanceof Map) {
        // { myvar : 'desc' }
        @SuppressWarnings("unchecked")
        Optional<Map.Entry<String,Object>> optional = ((Map<String,Object>)sort).entrySet().stream().findFirst();
        if (optional.isPresent()) {
          Map.Entry<String, Object> entry = optional.get();
          facetSort = new FacetRequest.FacetSort(entry.getKey(), FacetRequest.SortDirection.fromObj(entry.getValue()));
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Expected string/map for '" + sortParam +"', received "+ sort.getClass().getSimpleName() + "=" + sort);
      }

      Map<String, AggValueSource> facetStats = facet.facetStats;
      // validate facet sort
      boolean isValidSort = facetSort == null ||
              "index".equals(facetSort.sortVariable) ||
              "count".equals(facetSort.sortVariable) ||
              (facetStats != null && facetStats.containsKey(facetSort.sortVariable));

      if (!isValidSort) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Invalid " + sortParam + " option '" + sort + "' for field '" + facet.field + "'");
      }
      return facetSort;
    }

  }

}
