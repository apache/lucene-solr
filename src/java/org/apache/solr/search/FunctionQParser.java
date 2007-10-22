/**
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

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionQParser extends QParser {
  public FunctionQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  QueryParsing.StrParser sp;

  public Query parse() throws ParseException {
    sp = new QueryParsing.StrParser(getString());
    ValueSource vs = parseValSource();

    /***  boost promoted to top-level query type to avoid this hack 

    // HACK - if this is a boosted query wrapped in a value-source, return
    // that boosted query instead of a FunctionQuery
    if (vs instanceof QueryValueSource) {
      Query q = ((QueryValueSource)vs).getQuery();
      if (q instanceof BoostedQuery) return q;
    }
    ***/

    return new FunctionQuery(vs);
  }

  private abstract static class VSParser {
    abstract ValueSource parse(FunctionQParser fp) throws ParseException;
  }

  private static Map<String, VSParser> vsParsers = new HashMap<String, VSParser>();
  static {
    vsParsers.put("ord", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.sp.getId();
        return new OrdFieldSource(field);
      }
    });
    vsParsers.put("rord", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        String field = fp.sp.getId();
        return new ReverseOrdFieldSource(field);
      }
    });
    vsParsers.put("linear", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        fp.sp.expect(",");
        float slope = fp.sp.getFloat();
        fp.sp.expect(",");
        float intercept = fp.sp.getFloat();
        return new LinearFloatFunction(source,slope,intercept);
      }
    });
    vsParsers.put("max", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        fp.sp.expect(",");
        float val = fp.sp.getFloat();
        return new MaxFloatFunction(source,val);
      }
    });
    vsParsers.put("recip", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        fp.sp.expect(",");
        float m = fp.sp.getFloat();
        fp.sp.expect(",");
        float a = fp.sp.getFloat();
        fp.sp.expect(",");
        float b = fp.sp.getFloat();
        return new ReciprocalFloatFunction(source,m,a,b);
      }
    });
    vsParsers.put("scale", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        fp.sp.expect(",");
        float min = fp.sp.getFloat();
        fp.sp.expect(",");
        float max = fp.sp.getFloat();
        return new ScaleFloatFunction(source,min,max);
      }
    });
    vsParsers.put("pow", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValSource();
        fp.sp.expect(",");
        ValueSource b = fp.parseValSource();
        return new PowFloatFunction(a,b);
      }
    });
    vsParsers.put("div", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource a = fp.parseValSource();
        fp.sp.expect(",");
        ValueSource b = fp.parseValSource();
        return new DivFloatFunction(a,b);
      }
    });
    vsParsers.put("map", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        fp.sp.expect(",");
        float min = fp.sp.getFloat();
        fp.sp.expect(",");
        float max = fp.sp.getFloat();
        fp.sp.expect(",");
        float target = fp.sp.getFloat();
        return new RangeMapFloatFunction(source,min,max,target);
      }
    });
    vsParsers.put("sqrt", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "sqrt";
          }
          protected float func(int doc, DocValues vals) {
            return (float)Math.sqrt(vals.floatVal(doc));
          }
        };
      }
    });
    vsParsers.put("log", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "log";
          }
          protected float func(int doc, DocValues vals) {
            return (float)Math.log10(vals.floatVal(doc));
          }
        };
      }
    });
    vsParsers.put("abs", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        ValueSource source = fp.parseValSource();
        return new SimpleFloatFunction(source) {
          protected String name() {
            return "abs";
          }
          protected float func(int doc, DocValues vals) {
            return (float)Math.abs(vals.floatVal(doc));
          }
        };
      }
    });
    vsParsers.put("sum", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new SumFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    vsParsers.put("product", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        List<ValueSource> sources = fp.parseValueSourceList();
        return new ProductFloatFunction(sources.toArray(new ValueSource[sources.size()]));
      }
    });
    vsParsers.put("query", new VSParser() {
      // boost(query($q),rating)
      ValueSource parse(FunctionQParser fp) throws ParseException {
        Query q = fp.getNestedQuery();
        float defVal = 0.0f;
        if (fp.sp.opt(",")) {
          defVal = fp.sp.getFloat();
        }
        return new QueryValueSource(q, defVal);
      }
    });
    vsParsers.put("boost", new VSParser() {
      ValueSource parse(FunctionQParser fp) throws ParseException {
        Query q = fp.getNestedQuery();
        fp.sp.expect(",");
        ValueSource vs = fp.parseValSource();
        BoostedQuery bq = new BoostedQuery(q, vs);
System.out.println("Constructed Boostedquery " + bq);
        return new QueryValueSource(bq, 0.0f);
      }
    });
  }

  private List<ValueSource> parseValueSourceList() throws ParseException {
    List<ValueSource> sources = new ArrayList<ValueSource>(3);
    for (;;) {
      sources.add(parseValSource());
      char ch = sp.peek();
      if (ch==')') break;
      sp.expect(",");
    }
    return sources;
  }

  private ValueSource parseValSource() throws ParseException {
    int ch = sp.peek();
    if (ch>='0' && ch<='9'  || ch=='.' || ch=='+' || ch=='-') {
      return new ConstValueSource(sp.getFloat());
    }

    String id = sp.getId();
    if (sp.opt("(")) {
      // a function... look it up.
      VSParser argParser = vsParsers.get(id);
      if (argParser==null) {
        throw new ParseException("Unknown function " + id + " in FunctionQuery(" + sp + ")");
      }
      ValueSource vs = argParser.parse(this);
      sp.expect(")");
      return vs;
    }

    SchemaField f = req.getSchema().getField(id);
    return f.getType().getValueSource(f, this);
  }

  private Query getNestedQuery() throws ParseException {
    if (sp.opt("$")) {
      String param = sp.getId();
      sp.pos += param.length();
      String qstr = getParam(param);
      qstr = qstr==null ? "" : qstr;
      return subQuery(qstr, null).parse();
    }

    int start = sp.pos;
    int end = sp.pos;
    String v = sp.val; 

    String qs = v.substring(start);
    HashMap nestedLocalParams = new HashMap<String,String>();
    end = QueryParsing.parseLocalParams(qs, start, nestedLocalParams, getParams());

    QParser sub;

    if (end>start) {
      if (nestedLocalParams.get(QueryParsing.V) != null) {
        // value specified directly in local params... so the end of the
        // query should be the end of the local params.
        sub = subQuery(qs.substring(0, end), null);
      } else {
        // value here is *after* the local params... ask the parser.
        sub = subQuery(qs, null);
        // int subEnd = sub.findEnd(')');
        // TODO.. implement functions to find the end of a nested query
        throw new ParseException("Nested local params must have value in v parameter.  got '" + qs + "'");
      }
    } else {
      throw new ParseException("Nested function query must use $param or <!v=value> forms. got '" + qs + "'");
    }

    sp.pos += end-start;  // advance past nested query
    return sub.getQuery();
  }

}
