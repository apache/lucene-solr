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

public class FunctionQParser extends QParser {

  protected QueryParsing.StrParser sp;

  public FunctionQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  public Query parse() throws ParseException {
    sp = new QueryParsing.StrParser(getString());
    ValueSource vs = parseValueSource();

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

  /**
   * Are there more arguments in the argument list being parsed?
   * 
   * @return whether more args exist
   * @throws ParseException
   */
  public boolean hasMoreArguments() throws ParseException {
    int ch = sp.peek();
    /* determine whether the function is ending with a paren or end of str */
    return (! (ch == 0 || ch == ')') );
  }
  
  /**
   * TODO: Doc
   * 
   * @return
   * @throws ParseException
   */
  public String parseId() throws ParseException {
    String value = sp.getId();
    consumeArgumentDelimiter();
    return value;
  }
  
  /**
   * Parse a float.
   * 
   * @return Float
   * @throws ParseException
   */
  public Float parseFloat() throws ParseException {
    float value = sp.getFloat();
    consumeArgumentDelimiter();
    return value;
  }
  
  /**
   * Parse a list of ValueSource.  Must be the final set of arguments
   * to a ValueSource.
   * 
   * @return List<ValueSource>
   * @throws ParseException
   */
  public List<ValueSource> parseValueSourceList() throws ParseException {
    List<ValueSource> sources = new ArrayList<ValueSource>(3);
    for (;;) {
      sources.add(parseValueSource(false));
      if (! consumeArgumentDelimiter()) break;
    }
    return sources;
  }

  /**
   * Parse an individual ValueSource.
   * 
   * @return
   * @throws ParseException
   */
  public ValueSource parseValueSource() throws ParseException {
    /* consume the delimiter afterward for an external call to parseValueSource */
    return parseValueSource(true);
  }
  
  /**
   * TODO: Doc
   * 
   * @return
   * @throws ParseException
   */
  public Query parseNestedQuery() throws ParseException {
    Query nestedQuery;
    
    if (sp.opt("$")) {
      String param = sp.getId();
      sp.pos += param.length();
      String qstr = getParam(param);
      qstr = qstr==null ? "" : qstr;
      nestedQuery = subQuery(qstr, null).parse();
    }
    else {
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
        throw new ParseException("Nested function query must use $param or {!v=value} forms. got '" + qs + "'");
      }
  
      sp.pos += end-start;  // advance past nested query
      nestedQuery = sub.getQuery();
    }
    consumeArgumentDelimiter();
    
    return nestedQuery;
  }

  /**
   * Parse an individual value source.
   * 
   * @param doConsumeDelimiter whether to consume a delimiter following the ValueSource  
   * @return
   * @throws ParseException
   */
  protected ValueSource parseValueSource(boolean doConsumeDelimiter) throws ParseException {
    ValueSource valueSource;
    
    int ch = sp.peek();
    if (ch>='0' && ch<='9'  || ch=='.' || ch=='+' || ch=='-') {
      valueSource = new ConstValueSource(sp.getFloat());
    }
    else {
      String id = sp.getId();
      if (sp.opt("(")) {
        // a function... look it up.
        ValueSourceParser argParser = req.getCore().getValueSourceParser(id);
        if (argParser==null) {
          throw new ParseException("Unknown function " + id + " in FunctionQuery(" + sp + ")");
        }
        valueSource = argParser.parse(this);
        sp.expect(")");
      }
      else {
        SchemaField f = req.getSchema().getField(id);
        valueSource = f.getType().getValueSource(f, this);
      }
    }
    
    if (doConsumeDelimiter)
      consumeArgumentDelimiter();
    
    return valueSource;
  }

  /**
   * Consume an argument delimiter (a comma) from the token stream.
   * Only consumes if more arguments should exist (no ending parens or end of string).
   * 
   * @return whether a delimiter was consumed
   * @throws ParseException
   */
  protected boolean consumeArgumentDelimiter() throws ParseException {
    /* if a list of args is ending, don't expect the comma */
    if (hasMoreArguments()) {
      sp.expect(",");
      return true;
    }
   
    return false;
  }
    

}
