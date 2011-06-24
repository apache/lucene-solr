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

  /** @lucene.internal */
  public QueryParsing.StrParser sp;
  boolean parseMultipleSources = true;
  boolean parseToEnd = true;

  public FunctionQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  public void setParseMultipleSources(boolean parseMultipleSources) {
    this.parseMultipleSources = parseMultipleSources;  
  }

  /** parse multiple comma separated value sources */
  public boolean getParseMultipleSources() {
    return parseMultipleSources;
  }

  public void setParseToEnd(boolean parseToEnd) {
    this.parseToEnd = parseToEnd;
  }

  /** throw exception if there is extra stuff at the end of the parsed valuesource(s). */
  public boolean getParseToEnd() {
    return parseMultipleSources;
  }

  @Override
  public Query parse() throws ParseException {
    sp = new QueryParsing.StrParser(getString());

    ValueSource vs = null;
    List<ValueSource> lst = null;

    for(;;) {
      ValueSource valsource = parseValueSource(false);
      sp.eatws();
      if (!parseMultipleSources) {
        vs = valsource; 
        break;
      } else {
        if (lst != null) {
          lst.add(valsource);
        } else {
          vs = valsource;
        }
      }

      // check if there is a "," separator
      if (sp.peek() != ',') break;

      consumeArgumentDelimiter();

      if (lst == null) {
        lst = new ArrayList<ValueSource>(2);
        lst.add(valsource);
      }
    }

    if (parseToEnd && sp.pos < sp.end) {
      throw new ParseException("Unexpected text after function: " + sp.val.substring(sp.pos, sp.end));
    }

    if (lst != null) {
      vs = new VectorValueSource(lst);
    }

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
   * @throws ParseException
   */
  public String parseId() throws ParseException {
    String value = parseArg();
    if (argWasQuoted) throw new ParseException("Expected identifier instead of quoted string:" + value);
    return value;
  }
  
  /**
   * Parse a float.
   * 
   * @return Float
   * @throws ParseException
   */
  public Float parseFloat() throws ParseException {
    String str = parseArg();
    if (argWasQuoted()) throw new ParseException("Expected float instead of quoted string:" + str);
    float value = Float.parseFloat(str);
    return value;
  }

  /**
   * Parse a Double
   * @return double
   * @throws ParseException
   */
  public double parseDouble() throws ParseException {
    String str = parseArg();
    if (argWasQuoted()) throw new ParseException("Expected double instead of quoted string:" + str);
    double value = Double.parseDouble(str);
    return value;
  }

  /**
   * Parse an integer
   * @return An int
   * @throws ParseException
   */
  public int parseInt() throws ParseException {
    String str = parseArg();
    if (argWasQuoted()) throw new ParseException("Expected double instead of quoted string:" + str);
    int value = Integer.parseInt(str);
    return value;
  }


  private boolean argWasQuoted;
  public boolean argWasQuoted() {
    return argWasQuoted;
  }

  public String parseArg() throws ParseException {
    argWasQuoted = false;

    sp.eatws();
    char ch = sp.peek();
    String val = null;
    switch (ch) {
      case ')': return null;
      case '$':
        sp.pos++;
        String param = sp.getId();
        val = getParam(param);
        break;
      case '\'':
      case '"':
        val = sp.getQuotedString();
        argWasQuoted = true;
        break;
      default:
        // read unquoted literal ended by whitespace ',' or ')'
        // there is no escaping.
        int valStart = sp.pos;
        for (;;) {
          if (sp.pos >= sp.end) {
            throw new ParseException("Missing end to unquoted value starting at " + valStart + " str='" + sp.val +"'");
          }
          char c = sp.val.charAt(sp.pos);
          if (c==')' || c==',' || Character.isWhitespace(c)) {
            val = sp.val.substring(valStart, sp.pos);
            break;
          }
          sp.pos++;
        }
    }

    sp.eatws();
    consumeArgumentDelimiter();
    return val;
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
    while (hasMoreArguments()) {
      sources.add(parseValueSource(true));
    }
    return sources;
  }

  /**
   * Parse an individual ValueSource.
   * 
   * @throws ParseException
   */
  public ValueSource parseValueSource() throws ParseException {
    /* consume the delimiter afterward for an external call to parseValueSource */
    return parseValueSource(true);
  }
  
  /**
   * TODO: Doc
   * 
   * @throws ParseException
   */
  public Query parseNestedQuery() throws ParseException {
    Query nestedQuery;
    
    if (sp.opt("$")) {
      String param = sp.getId();
      String qstr = getParam(param);
      qstr = qstr==null ? "" : qstr;
      nestedQuery = subQuery(qstr, null).getQuery();
    }
    else {
      int start = sp.pos;
      String v = sp.val;
  
      String qs = v;
      HashMap nestedLocalParams = new HashMap<String,String>();
      int end = QueryParsing.parseLocalParams(qs, start, nestedLocalParams, getParams());
  
      QParser sub;
  
      if (end>start) {
        if (nestedLocalParams.get(QueryParsing.V) != null) {
          // value specified directly in local params... so the end of the
          // query should be the end of the local params.
          sub = subQuery(qs.substring(start, end), null);
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
   * @throws ParseException
   */
  protected ValueSource parseValueSource(boolean doConsumeDelimiter) throws ParseException {
    ValueSource valueSource;
    
    int ch = sp.peek();
    if (ch>='0' && ch<='9'  || ch=='.' || ch=='+' || ch=='-') {
      Number num = sp.getNumber();
      if (num instanceof Long) {
        valueSource = new LongConstValueSource(num.longValue());
      } else if (num instanceof Double) {
        valueSource = new DoubleConstValueSource(num.doubleValue());
      } else {
        // shouldn't happen
        valueSource = new ConstValueSource(num.floatValue());
      }
    } else if (ch == '"' || ch == '\''){
      valueSource = new LiteralValueSource(sp.getQuotedString());
    } else if (ch == '$') {
      sp.pos++;
      String param = sp.getId();
      String val = getParam(param);
      if (val == null) {
        throw new ParseException("Missing param " + param + " while parsing function '" + sp.val + "'");
      }

      QParser subParser = subQuery(val, "func");
      if (subParser instanceof FunctionQParser) {
        ((FunctionQParser)subParser).setParseMultipleSources(true);
      }
      Query subQuery = subParser.getQuery();
      if (subQuery instanceof FunctionQuery) {
        valueSource = ((FunctionQuery) subQuery).getValueSource();
      } else {
        valueSource = new QueryValueSource(subQuery, 0.0f);
      }

      /***
       // dereference *simple* argument (i.e., can't currently be a function)
       // In the future we could support full function dereferencing via a stack of ValueSource (or StringParser) objects
      ch = val.length()==0 ? '\0' : val.charAt(0);

      if (ch>='0' && ch<='9'  || ch=='.' || ch=='+' || ch=='-') {
        QueryParsing.StrParser sp = new QueryParsing.StrParser(val);
        Number num = sp.getNumber();
        if (num instanceof Long) {
          valueSource = new LongConstValueSource(num.longValue());
        } else if (num instanceof Double) {
          valueSource = new DoubleConstValueSource(num.doubleValue());
        } else {
          // shouldn't happen
          valueSource = new ConstValueSource(num.floatValue());
        }
      } else if (ch == '"' || ch == '\'') {
        QueryParsing.StrParser sp = new QueryParsing.StrParser(val);
        val = sp.getQuotedString();
        valueSource = new LiteralValueSource(val);
      } else {
        if (val.length()==0) {
          valueSource = new LiteralValueSource(val);
        } else {
          String id = val;
          SchemaField f = req.getSchema().getField(id);
          valueSource = f.getType().getValueSource(f, this);
        }
      }
       ***/

    } else {

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
        if ("true".equals(id)) {
          valueSource = new BoolConstValueSource(true);
        } else if ("false".equals(id)) {
          valueSource = new BoolConstValueSource(false);
        } else {
          SchemaField f = req.getSchema().getField(id);
          valueSource = f.getType().getValueSource(f, this);
        }
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
