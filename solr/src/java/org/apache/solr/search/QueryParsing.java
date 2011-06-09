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

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FunctionQuery;
import org.apache.solr.search.function.QueryValueSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collection of static utilities useful for query parsing.
 *
 * @version $Id$
 */
public class QueryParsing {
  public static final String OP = "q.op";  // the SolrParam used to override the QueryParser "default operator"
  public static final String V = "v";      // value of this parameter
  public static final String F = "f";      // field that a query or command pertains to
  public static final String TYPE = "type";// type of this query or command
  public static final String DEFTYPE = "defType"; // default type for any direct subqueries
  public static final String LOCALPARAM_START = "{!";
  public static final char LOCALPARAM_END = '}';
  public static final String DOCID = "_docid_";
  public static final String SCORE = "score";

  // true if the value was specified by the "v" param (i.e. v=myval, or v=$param)
  public static final String VAL_EXPLICIT = "__VAL_EXPLICIT__";


  /**
   * Returns the "prefered" default operator for use by Query Parsers, 
   * based on the settings in the IndexSchema which may be overridden using 
   * an optional String override value.
   *
   * @see IndexSchema#getQueryParserDefaultOperator()
   * @see #OP
   */
  public static Operator getQueryParserDefaultOperator(final IndexSchema sch, 
                                                       final String override) {
    String val = override;
    if (null == val) val = sch.getQueryParserDefaultOperator();
    return "AND".equals(val) ? Operator.AND : Operator.OR;
  }


  // note to self: something needs to detect infinite recursion when parsing queries
  public static int parseLocalParams(String txt, int start, Map<String, String> target, SolrParams params) throws ParseException {
    return parseLocalParams(txt, start, target, params, LOCALPARAM_START, LOCALPARAM_END);
  }


  public static int parseLocalParams(String txt, int start, Map<String, String> target, SolrParams params, String startString, char endChar) throws ParseException {
    int off = start;
    if (!txt.startsWith(startString, off)) return start;
    StrParser p = new StrParser(txt, start, txt.length());
    p.pos += startString.length(); // skip over "{!"

    for (; ;) {
      /*
      if (p.pos>=txt.length()) {
        throw new ParseException("Missing '}' parsing local params '" + txt + '"');
      }
      */
      char ch = p.peek();
      if (ch == endChar) {
        return p.pos + 1;
      }

      String id = p.getId();
      if (id.length() == 0) {
        throw new ParseException("Expected ending character '" + endChar + "' parsing local params '" + txt + '"');

      }
      String val = null;

      ch = p.peek();
      if (ch != '=') {
        // single word... treat {!func} as type=func for easy lookup
        val = id;
        id = TYPE;
      } else {
        // saw equals, so read value
        p.pos++;
        ch = p.peek();
        boolean deref = false;
        if (ch == '$') {
          p.pos++;
          ch = p.peek();
          deref = true;  // dereference whatever value is read by treating it as a variable name
        }

        if (ch == '\"' || ch == '\'') {
          val = p.getQuotedString();
        } else {
          // read unquoted literal ended by whitespace or endChar (normally '}')
          // there is no escaping.
          int valStart = p.pos;
          for (; ;) {
            if (p.pos >= p.end) {
              throw new ParseException("Missing end to unquoted value starting at " + valStart + " str='" + txt + "'");
            }
            char c = p.val.charAt(p.pos);
            if (c == endChar || Character.isWhitespace(c)) {
              val = p.val.substring(valStart, p.pos);
              break;
            }
            p.pos++;
          }
        }

        if (deref) {  // dereference parameter
          if (params != null) {
            val = params.get(val);
          }
        }
      }
      if (target != null) target.put(id, val);
    }
  }


  public static String encodeLocalParamVal(String val) {
    int len = val.length();
    int i = 0;
    if (len > 0 && val.charAt(0) != '$') {
      for (;i<len; i++) {
        char ch = val.charAt(i);
        if (Character.isWhitespace(ch) || ch=='}') break;
      }
    }

    if (i>=len) return val;

    // We need to enclose in quotes... but now we need to escape
    StringBuilder sb = new StringBuilder(val.length() + 4);
    sb.append('\'');
    for (i=0; i<len; i++) {
      char ch = val.charAt(i);
      if (ch=='\'') {
        sb.append('\\');
      }
      sb.append(ch);
    }
    sb.append('\'');
    return sb.toString();
  }
  

  /**
   * "foo" returns null
   * "{!prefix f=myfield}yes" returns type="prefix",f="myfield",v="yes"
   * "{!prefix f=myfield v=$p}" returns type="prefix",f="myfield",v=params.get("p")
   */
  public static SolrParams getLocalParams(String txt, SolrParams params) throws ParseException {
    if (txt == null || !txt.startsWith(LOCALPARAM_START)) {
      return null;
    }
    Map<String, String> localParams = new HashMap<String, String>();
    int start = QueryParsing.parseLocalParams(txt, 0, localParams, params);

    String val = localParams.get(V);
    if (val == null) {
      val = txt.substring(start);
      localParams.put(V, val);
    } else {
      // localParams.put(VAL_EXPLICIT, "true");
    }
    return new MapSolrParams(localParams);
  }


  /**
   * Returns null if the sortSpec is the standard sort desc.
   * <p/>
   * <p>
   * The form of the sort specification string currently parsed is:
   * </p>
   * <pre>>
   * SortSpec ::= SingleSort [, SingleSort]*
   * SingleSort ::= <fieldname> SortDirection
   * SortDirection ::= top | desc | bottom | asc
   * </pre>
   * Examples:
   * <pre>
   *   score desc               #normal sort by score (will return null)
   *   weight bottom            #sort by weight ascending
   *   weight desc              #sort by weight descending
   *   height desc,weight desc  #sort by height descending, and use weight descending to break any ties
   *   height desc,weight asc   #sort by height descending, using weight ascending as a tiebreaker
   * </pre>
   */
  public static Sort parseSort(String sortSpec, SolrQueryRequest req) {
    if (sortSpec == null || sortSpec.length() == 0) return null;
    List<SortField> lst = new ArrayList<SortField>(4);

    try {

      StrParser sp = new StrParser(sortSpec);
      while (sp.pos < sp.end) {
        sp.eatws();

        final int start = sp.pos;

        // short circuit test for a really simple field name
        String field = sp.getId(null);
        ParseException qParserException = null;

        if (field == null || sp.ch() != ' ') {
          // let's try it as a function instead
          String funcStr = sp.val.substring(start);

          QParser parser = QParser.getParser(funcStr, FunctionQParserPlugin.NAME, req);
          Query q = null;
          try {
            if (parser instanceof FunctionQParser) {
              FunctionQParser fparser = (FunctionQParser)parser;
              fparser.setParseMultipleSources(false);
              fparser.setParseToEnd(false);
              
              q = fparser.getQuery();
              
              if (fparser.localParams != null) {
                if (fparser.valFollowedParams) {
                  // need to find the end of the function query via the string parser
                  int leftOver = fparser.sp.end - fparser.sp.pos;
                  sp.pos = sp.end - leftOver;   // reset our parser to the same amount of leftover
                } else {
                  // the value was via the "v" param in localParams, so we need to find
                  // the end of the local params themselves to pick up where we left off
                  sp.pos = start + fparser.localParamsEnd;
                }
              } else {
                // need to find the end of the function query via the string parser
                int leftOver = fparser.sp.end - fparser.sp.pos;
                sp.pos = sp.end - leftOver;   // reset our parser to the same amount of leftover
              }
            } else {
              // A QParser that's not for function queries.
              // It must have been specified via local params.
              q = parser.getQuery();

              assert parser.getLocalParams() != null;
              sp.pos = start + parser.localParamsEnd;
            }

            Boolean top = sp.getSortDirection();
            if (null != top) {
              // we have a Query and a valid direction
              if (q instanceof FunctionQuery) {
                lst.add(((FunctionQuery)q).getValueSource().getSortField(top));
              } else {
                lst.add((new QueryValueSource(q, 0.0f)).getSortField(top));
              }
              continue;
            }
          } catch (ParseException e) {
            // hang onto this in case the string isn't a full field name either
            qParserException = e;
          }
        }

        // if we made it here, we either have a "simple" field name,
        // or there was a problem parsing the string as a complex func/quer

        if (field == null) {
          // try again, simple rules for a field name with no whitespace
          sp.pos = start;
          field = sp.getSimpleString();
        }
        Boolean top = sp.getSortDirection();
        if (null == top) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                                    "Can't determine a Sort Order (asc or desc) in sort spec " + sp);
        }
        
        if (SCORE.equals(field)) {
          if (top) {
            lst.add(SortField.FIELD_SCORE);
          } else {
            lst.add(new SortField(null, SortField.SCORE, true));
          }
        } else if (DOCID.equals(field)) {
          lst.add(new SortField(null, SortField.DOC, top));
        } else {
          // try to find the field
          SchemaField sf = req.getSchema().getFieldOrNull(field);
          if (null == sf) {
            if (null != qParserException) {
              throw new SolrException
                (SolrException.ErrorCode.BAD_REQUEST,
                 "sort param could not be parsed as a query, and is not a "+
                 "field that exists in the index: " + field,
                 qParserException);
            }
            throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
               "sort param field can't be found: " + field);
          }
          lst.add(sf.getSortField(top));
        }
      }

    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "error in sort: " + sortSpec, e);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "error in sort: " + sortSpec, e);
    }


    // normalize a sort on score desc to null
    if (lst.size()==1 && lst.get(0) == SortField.FIELD_SCORE) {
      return null;
    }

    return new Sort(lst.toArray(new SortField[lst.size()]));
  }



  ///////////////////////////
  ///////////////////////////
  ///////////////////////////

  static FieldType writeFieldName(String name, IndexSchema schema, Appendable out, int flags) throws IOException {
    FieldType ft = null;
    ft = schema.getFieldTypeNoEx(name);
    out.append(name);
    if (ft == null) {
      out.append("(UNKNOWN FIELD " + name + ')');
    }
    out.append(':');
    return ft;
  }

  static void writeFieldVal(String val, FieldType ft, Appendable out, int flags) throws IOException {
    if (ft != null) {
      try {
        out.append(ft.indexedToReadable(val));
      } catch (Exception e) {
        out.append("EXCEPTION(val=");
        out.append(val);
        out.append(")");
      }
    } else {
      out.append(val);
    }
  }

  static void writeFieldVal(BytesRef val, FieldType ft, Appendable out, int flags) throws IOException {
    if (ft != null) {
      try {
        CharsRef readable = new CharsRef();
        ft.indexedToReadable(val, readable);
        out.append(readable);
      } catch (Exception e) {
        out.append("EXCEPTION(val=");
        out.append(val.utf8ToString());
        out.append(")");
      }
    } else {
      out.append(val.utf8ToString());
    }
  }

  /**
   * @see #toString(Query,IndexSchema)
   */
  public static void toString(Query query, IndexSchema schema, Appendable out, int flags) throws IOException {
    boolean writeBoost = true;

    if (query instanceof TermQuery) {
      TermQuery q = (TermQuery) query;
      Term t = q.getTerm();
      FieldType ft = writeFieldName(t.field(), schema, out, flags);
      writeFieldVal(t.bytes(), ft, out, flags);
    } else if (query instanceof TermRangeQuery) {
      TermRangeQuery q = (TermRangeQuery) query;
      String fname = q.getField();
      FieldType ft = writeFieldName(fname, schema, out, flags);
      out.append(q.includesLower() ? '[' : '{');
      BytesRef lt = q.getLowerTerm();
      BytesRef ut = q.getUpperTerm();
      if (lt == null) {
        out.append('*');
      } else {
        writeFieldVal(lt, ft, out, flags);
      }

      out.append(" TO ");

      if (ut == null) {
        out.append('*');
      } else {
        writeFieldVal(ut, ft, out, flags);
      }

      out.append(q.includesUpper() ? ']' : '}');
    } else if (query instanceof NumericRangeQuery) {
      NumericRangeQuery q = (NumericRangeQuery) query;
      String fname = q.getField();
      FieldType ft = writeFieldName(fname, schema, out, flags);
      out.append(q.includesMin() ? '[' : '{');
      Number lt = q.getMin();
      Number ut = q.getMax();
      if (lt == null) {
        out.append('*');
      } else {
        out.append(lt.toString());
      }

      out.append(" TO ");

      if (ut == null) {
        out.append('*');
      } else {
        out.append(ut.toString());
      }

      out.append(q.includesMax() ? ']' : '}');
    } else if (query instanceof BooleanQuery) {
      BooleanQuery q = (BooleanQuery) query;
      boolean needParens = false;

      if (q.getBoost() != 1.0 || q.getMinimumNumberShouldMatch() != 0) {
        needParens = true;
      }
      if (needParens) {
        out.append('(');
      }
      boolean first = true;
      for (BooleanClause c : q.clauses()) {
        if (!first) {
          out.append(' ');
        } else {
          first = false;
        }

        if (c.isProhibited()) {
          out.append('-');
        } else if (c.isRequired()) {
          out.append('+');
        }
        Query subQuery = c.getQuery();
        boolean wrapQuery = false;

        // TODO: may need to put parens around other types
        // of queries too, depending on future syntax.
        if (subQuery instanceof BooleanQuery) {
          wrapQuery = true;
        }

        if (wrapQuery) {
          out.append('(');
        }

        toString(subQuery, schema, out, flags);

        if (wrapQuery) {
          out.append(')');
        }
      }

      if (needParens) {
        out.append(')');
      }
      if (q.getMinimumNumberShouldMatch() > 0) {
        out.append('~');
        out.append(Integer.toString(q.getMinimumNumberShouldMatch()));
      }

    } else if (query instanceof PrefixQuery) {
      PrefixQuery q = (PrefixQuery) query;
      Term prefix = q.getPrefix();
      FieldType ft = writeFieldName(prefix.field(), schema, out, flags);
      out.append(prefix.text());
      out.append('*');
    } else if (query instanceof WildcardQuery) {
      out.append(query.toString());
      writeBoost = false;
    } else if (query instanceof FuzzyQuery) {
      out.append(query.toString());
      writeBoost = false;
    } else if (query instanceof ConstantScoreQuery) {
      out.append(query.toString());
      writeBoost = false;
    } else {
      out.append(query.getClass().getSimpleName()
              + '(' + query.toString() + ')');
      writeBoost = false;
    }

    if (writeBoost && query.getBoost() != 1.0f) {
      out.append("^");
      out.append(Float.toString(query.getBoost()));
    }

  }

  /**
   * Formats a Query for debugging, using the IndexSchema to make
   * complex field types readable.
   * <p/>
   * <p>
   * The benefit of using this method instead of calling
   * <code>Query.toString</code> directly is that it knows about the data
   * types of each field, so any field which is encoded in a particularly
   * complex way is still readable. The downside is that it only knows
   * about built in Query types, and will not be able to format custom
   * Query classes.
   * </p>
   */
  public static String toString(Query query, IndexSchema schema) {
    try {
      StringBuilder sb = new StringBuilder();
      toString(query, schema, sb, 0);
      return sb.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Simple class to help with parsing a string
   * <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
   */
  public static class StrParser {
    String val;
    int pos;
    int end;

    public StrParser(String val) {
      this(val, 0, val.length());
    }

    public StrParser(String val, int start, int end) {
      this.val = val;
      this.pos = start;
      this.end = end;
    }

    void eatws() {
      while (pos < end && Character.isWhitespace(val.charAt(pos))) pos++;
    }

    char ch() {
      return pos < end ? val.charAt(pos) : 0;
    }

    void skip(int nChars) {
      pos = Math.max(pos + nChars, end);
    }

    boolean opt(String s) {
      eatws();
      int slen = s.length();
      if (val.regionMatches(pos, s, 0, slen)) {
        pos += slen;
        return true;
      }
      return false;
    }

    boolean opt(char ch) {
      eatws();
      if (pos < end && val.charAt(pos) == ch) {
        pos++;
        return true;
      }
      return false;
    }


    void expect(String s) throws ParseException {
      eatws();
      int slen = s.length();
      if (val.regionMatches(pos, s, 0, slen)) {
        pos += slen;
      } else {
        throw new ParseException("Expected '" + s + "' at position " + pos + " in '" + val + "'");
      }
    }

    float getFloat() throws ParseException {
      eatws();
      char[] arr = new char[end - pos];
      int i;
      for (i = 0; i < arr.length; i++) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9')
                || ch == '+' || ch == '-'
                || ch == '.' || ch == 'e' || ch == 'E'
                ) {
          pos++;
          arr[i] = ch;
        } else {
          break;
        }
      }

      return Float.parseFloat(new String(arr, 0, i));
    }

    Number getNumber() throws ParseException {
      eatws();
      int start = pos;
      boolean flt = false;

      while (pos < end) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9') || ch == '+' || ch == '-') {
          pos++;
        } else if (ch == '.' || ch =='e' || ch=='E') {
          flt = true;
          pos++;
        } else {
          break;
        }
      }

      String v = val.substring(start,pos);
      if (flt) {
        return Double.parseDouble(v);
      } else {
        return Long.parseLong(v);
      }
    }

    double getDouble() throws ParseException {
      eatws();
      char[] arr = new char[end - pos];
      int i;
      for (i = 0; i < arr.length; i++) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9')
                || ch == '+' || ch == '-'
                || ch == '.' || ch == 'e' || ch == 'E'
                ) {
          pos++;
          arr[i] = ch;
        } else {
          break;
        }
      }

      return Double.parseDouble(new String(arr, 0, i));
    }

    int getInt() throws ParseException {
      eatws();
      char[] arr = new char[end - pos];
      int i;
      for (i = 0; i < arr.length; i++) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9')
                || ch == '+' || ch == '-'
                ) {
          pos++;
          arr[i] = ch;
        } else {
          break;
        }
      }

      return Integer.parseInt(new String(arr, 0, i));
    }


    String getId() throws ParseException {
      return getId("Expected identifier");
    }

    String getId(String errMessage) throws ParseException {
      eatws();
      int id_start = pos;
      char ch;
      if (pos < end && (ch = val.charAt(pos)) != '$' && Character.isJavaIdentifierStart(ch)) {
        pos++;
        while (pos < end) {
          ch = val.charAt(pos);
//          if (!Character.isJavaIdentifierPart(ch) && ch != '.' && ch != ':') {
          if (!Character.isJavaIdentifierPart(ch) && ch != '.') {
            break;
          }
          pos++;
        }
        return val.substring(id_start, pos);
      }

      if (errMessage != null) {
        throw new ParseException(errMessage + " at pos " + pos + " str='" + val + "'");
      }
      return null;
    }

    public String getGlobbedId(String errMessage) throws ParseException {
      eatws();
      int id_start = pos;
      char ch;
      if (pos < end && (ch = val.charAt(pos)) != '$' && (Character.isJavaIdentifierStart(ch) || ch=='?' || ch=='*')) {
        pos++;
        while (pos < end) {
          ch = val.charAt(pos);
          if (!(Character.isJavaIdentifierPart(ch) || ch=='?' || ch=='*') && ch != '.') {
            break;
          }
          pos++;
        }
        return val.substring(id_start, pos);
      }

      if (errMessage != null) {
        throw new ParseException(errMessage + " at pos " + pos + " str='" + val + "'");
      }
      return null;
    }

    /**
     * Skips leading whitespace and returns whatever sequence of non 
     * whitespace it can find (or hte empty string)
     */
    String getSimpleString() {
      eatws();
      int startPos = pos;
      char ch;
      while (pos < end) {
        ch = val.charAt(pos);
        if (Character.isWhitespace(ch)) break;
        pos++;
      }
      return val.substring(startPos, pos);
    }

    /**
     * Sort direction or null if current position does not inidcate a 
     * sort direction. (True is desc, False is asc).  
     * Position is advanced to after the comma (or end) when result is non null 
     */
    Boolean getSortDirection() throws ParseException {
      final int startPos = pos;
      final String order = getId(null);

      Boolean top = null;

      if (null != order) {
        if ("desc".equals(order) || "top".equals(order)) {
          top = true;
        } else if ("asc".equals(order) || "bottom".equals(order)) {
          top = false;
        }

        // it's not a legal direction if more stuff comes after it
        eatws();
        final char c = ch();
        if (0 == c) {
          // :NOOP
        } else if (',' == c) {
          pos++;
        } else {
          top = null;
        }
      }

      if (null == top) pos = startPos; // no direction, reset
      return top;
    }

    // return null if not a string
    String getQuotedString() throws ParseException {
      eatws();
      char delim = peekChar();
      if (!(delim == '\"' || delim == '\'')) {
        return null;
      }
      int val_start = ++pos;
      StringBuilder sb = new StringBuilder(); // needed for escaping
      for (; ;) {
        if (pos >= end) {
          throw new ParseException("Missing end quote for string at pos " + (val_start - 1) + " str='" + val + "'");
        }
        char ch = val.charAt(pos);
        if (ch == '\\') {
          pos++;
          if (pos >= end) break;
          ch = val.charAt(pos);
          switch (ch) {
            case 'n':
              ch = '\n';
              break;
            case 't':
              ch = '\t';
              break;
            case 'r':
              ch = '\r';
              break;
            case 'b':
              ch = '\b';
              break;
            case 'f':
              ch = '\f';
              break;
            case 'u':
              if (pos + 4 >= end) {
                throw new ParseException("bad unicode escape \\uxxxx at pos" + (val_start - 1) + " str='" + val + "'");
              }
              ch = (char) Integer.parseInt(val.substring(pos + 1, pos + 5), 16);
              pos += 4;
              break;
          }
        } else if (ch == delim) {
          pos++;  // skip over the quote
          break;
        }
        sb.append(ch);
        pos++;
      }

      return sb.toString();
    }

    // next non-whitespace char
    char peek() {
      eatws();
      return pos < end ? val.charAt(pos) : 0;
    }

    // next char
    char peekChar() {
      return pos < end ? val.charAt(pos) : 0;
    }

    @Override
    public String toString() {
      return "'" + val + "'" + ", pos=" + pos;
    }

  }

  /**
   * Builds a list of String which are stringified versions of a list of Queries
   */
  public static List<String> toString(List<Query> queries, IndexSchema schema) {
    List<String> out = new ArrayList<String>(queries.size());
    for (Query q : queries) {
      out.add(QueryParsing.toString(q, schema));
    }
    return out;
  }

}
