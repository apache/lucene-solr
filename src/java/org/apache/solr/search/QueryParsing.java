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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.function.FunctionQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Collection of static utilities useful for query parsing.
 *
 * @version $Id$
 */
public class QueryParsing {
  /** the SolrParam used to override the QueryParser "default operator" */
  public static final String OP = "q.op";
  public static final String V = "v";      // value of this parameter
  public static final String F = "f";      // field that a query or command pertains to
  public static final String TYPE = "type";// type of this query or command
  public static final String DEFTYPE = "defType"; // default type for any direct subqueries
  public static final String LOCALPARAM_START = "{!";
  public static final char LOCALPARAM_END = '}';

  /** 
   * Helper utility for parsing a query using the Lucene QueryParser syntax. 
   * @param qs query expression in standard Lucene syntax
   * @param schema used for default operator (overridden by params) and passed to the query parser for field format analysis information
   */
  public static Query parseQuery(String qs, IndexSchema schema) {
    return parseQuery(qs, null, schema);
  }

  /** 
   * Helper utility for parsing a query using the Lucene QueryParser syntax. 
   * @param qs query expression in standard Lucene syntax
   * @param defaultField default field used for unqualified search terms in the query expression
   * @param schema used for default operator (overridden by params) and passed to the query parser for field format analysis information
   */
  public static Query parseQuery(String qs, String defaultField, IndexSchema schema) {
    try {
      Query query = schema.getSolrQueryParser(defaultField).parse(qs);

      if (SolrCore.log.isTraceEnabled() ) {
        SolrCore.log.trace("After QueryParser:" + query);
      }

      return query;

    } catch (ParseException e) {
      SolrCore.log(e);
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Error parsing Lucene query",e);
    }
  }

  /**
   * Helper utility for parsing a query using the Lucene QueryParser syntax. 
   * @param qs query expression in standard Lucene syntax
   * @param defaultField default field used for unqualified search terms in the query expression
   * @param params used to determine the default operator, overriding the schema specified operator
   * @param schema used for default operator (overridden by params) and passed to the query parser for field format analysis information
   */
  public static Query parseQuery(String qs, String defaultField, SolrParams params, IndexSchema schema) {
    try {
      SolrQueryParser parser = schema.getSolrQueryParser(defaultField);
      String opParam = params.get(OP);
      if (opParam != null) {
        parser.setDefaultOperator("AND".equals(opParam) ? QueryParser.Operator.AND : QueryParser.Operator.OR);
      }
      Query query = parser.parse(qs);

      if (SolrCore.log.isTraceEnabled() ) {
        SolrCore.log.trace("After QueryParser:" + query);
      }

      return query;

    } catch (ParseException e) {
      SolrCore.log(e);
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Query parsing error: " + e.getMessage(),e);
    }
  }


  // note to self: something needs to detect infinite recursion when parsing queries
  static int parseLocalParams(String txt, int start, Map<String,String> target, SolrParams params) throws ParseException {
    int off=start;
    if (!txt.startsWith(LOCALPARAM_START,off)) return start;
    StrParser p = new StrParser(txt,start,txt.length());
    p.pos+=2; // skip over "{!"

    for(;;) {
      /*
      if (p.pos>=txt.length()) {
        throw new ParseException("Missing '}' parsing local params '" + txt + '"');
      }
      */
      char ch = p.peek();
      if (ch==LOCALPARAM_END) {
        return p.pos+1;
      }

      String id = p.getId();
      if (id.length()==0) {
        throw new ParseException("Expected identifier '}' parsing local params '" + txt + '"');

      }
      String val=null;

      ch = p.peek();
      if (ch!='=') {
        // single word... treat {!func} as type=func for easy lookup
        val = id;
        id = TYPE;
      } else {
        // saw equals, so read value
        p.pos++;
        ch = p.peek();
        if (ch=='\"' || ch=='\'') {
          val = p.getQuotedString();
        } else if (ch=='$') {
          p.pos++;
          // dereference parameter
          String pname = p.getId();
          if (params!=null) {
            val = params.get(pname);
          }
        } else {
          // read unquoted literal ended by whitespace or '}'
          // there is no escaping.
          int valStart = p.pos;
          for (;;) {
            if (p.pos >= p.end) {
              throw new ParseException("Missing end to unquoted value starting at " + valStart + " str='" + txt +"'");
            }
            char c = p.val.charAt(p.pos);
            if (c==LOCALPARAM_END || Character.isWhitespace(c)) {
              val = p.val.substring(valStart, p.pos);
              break;
            }
            p.pos++;
          }
        }
      }
      if (target != null) target.put(id,val);
    }
  }

  /**
   *  "foo" returns null
   *  "{!prefix f=myfield}yes" returns type="prefix",f="myfield",v="yes"
   *  "{!prefix f=myfield v=$p}" returns type="prefix",f="myfield",v=params.get("p")
   */
  public static SolrParams getLocalParams(String txt, SolrParams params) throws ParseException {
    if (txt==null || !txt.startsWith(LOCALPARAM_START)) {
      return null;      
    }
    Map<String,String> localParams = new HashMap<String,String>();
    int start = QueryParsing.parseLocalParams(txt, 0, localParams, params);

    String val;
    if (start >= txt.length()) {
      // if the rest of the string is empty, check for "v" to provide the value
      val = localParams.get(V);
      val = val==null ? "" : val;
    } else {
      val = txt.substring(start);
    }
    localParams.put(V,val);
    return new MapSolrParams(localParams);
  }


  private static Pattern sortSep = Pattern.compile(",");

  /**
   * Returns null if the sortSpec is the standard sort desc.
   *
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
   *
   */
  public static Sort parseSort(String sortSpec, IndexSchema schema) {
    if (sortSpec==null || sortSpec.length()==0) return null;

    String[] parts = sortSep.split(sortSpec.trim());
    if (parts.length == 0) return null;

    SortField[] lst = new SortField[parts.length];
    for( int i=0; i<parts.length; i++ ) {
      String part = parts[i].trim();
      boolean top=true;
        
      int idx = part.indexOf( ' ' );
      if( idx > 0 ) {
        String order = part.substring( idx+1 ).trim();
      	if( "desc".equals( order ) || "top".equals(order) ) {
      	  top = true;
      	}
      	else if ("asc".equals(order) || "bottom".equals(order)) {
      	  top = false;
      	}
      	else {
      	  throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Unknown sort order: "+order);
      	}
      	part = part.substring( 0, idx ).trim();
      }
      else {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Missing sort order." );
      }
    	
      if( "score".equals(part) ) {
        if (top) {
          // If there is only one thing in the list, just do the regular thing...
          if( parts.length == 1 ) {
            return null; // do normal scoring...
          }
          lst[i] = SortField.FIELD_SCORE;
        }
        else {
          lst[i] = new SortField(null, SortField.SCORE, true);
        }
      } 
      else {
        // getField could throw an exception if the name isn't found
        SchemaField f = null;
        try{
          f = schema.getField(part);
        }
        catch( SolrException e ){
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "can not sort on undefined field: "+part, e );
        }
        if (f == null || !f.indexed()){
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "can not sort on unindexed field: "+part );
        }
        lst[i] = f.getType().getSortField(f,top);
      }
    }
    return new Sort(lst);
  }


  ///////////////////////////
  ///////////////////////////
  ///////////////////////////

  static FieldType writeFieldName(String name, IndexSchema schema, Appendable out, int flags) throws IOException {
    FieldType ft = null;
    ft = schema.getFieldTypeNoEx(name);
    out.append(name);
    if (ft==null) {
      out.append("(UNKNOWN FIELD "+name+')');
    }
    out.append(':');
    return ft;
  }

  static void writeFieldVal(String val, FieldType ft, Appendable out, int flags) throws IOException {
    if (ft!=null) {
      out.append(ft.toExternal(new Field("",val, Field.Store.YES, Field.Index.UN_TOKENIZED)));
    } else {
      out.append(val);
    }
  }
  /** @see #toString(Query,IndexSchema) */
  public static void toString(Query query, IndexSchema schema, Appendable out, int flags) throws IOException {
    boolean writeBoost=true;

    if (query instanceof TermQuery) {
      TermQuery q = (TermQuery)query;
      Term t = q.getTerm();
      FieldType ft = writeFieldName(t.field(), schema, out, flags);
      writeFieldVal(t.text(), ft, out, flags);
    } else if (query instanceof TermRangeQuery) {
      TermRangeQuery q = (TermRangeQuery)query;
      String fname = q.getField();
      FieldType ft = writeFieldName(fname, schema, out, flags);
      out.append( q.includesLower() ? '[' : '{' );
      String lt = q.getLowerTerm();
      String ut = q.getUpperTerm();
      if (lt==null) {
        out.append('*');
      } else {
        writeFieldVal(lt, ft, out, flags);
      }

      out.append(" TO ");

      if (ut==null) {
        out.append('*');
      } else {
        writeFieldVal(ut, ft, out, flags);
      }

      out.append( q.includesUpper() ? ']' : '}' );
    } else if (query instanceof NumericRangeQuery) {
      NumericRangeQuery q = (NumericRangeQuery)query;
      String fname = q.getField();
      FieldType ft = writeFieldName(fname, schema, out, flags);
      out.append( q.includesMin() ? '[' : '{' );
      Number lt = q.getMin();
      Number ut = q.getMax();
      if (lt==null) {
        out.append('*');
      } else {
        writeFieldVal(lt.toString(), ft, out, flags);
      }

      out.append(" TO ");

      if (ut==null) {
        out.append('*');
      } else {
        writeFieldVal(ut.toString(), ft, out, flags);
      }

      out.append( q.includesMax() ? ']' : '}' );
    } else if (query instanceof BooleanQuery) {
      BooleanQuery q = (BooleanQuery)query;
      boolean needParens=false;

      if (q.getBoost() != 1.0 || q.getMinimumNumberShouldMatch() != 0) {
        needParens=true;
      }
      if (needParens) {
        out.append('(');
      }
      boolean first=true;
      for (BooleanClause c : (List<BooleanClause>)q.clauses()) {
        if (!first) {
          out.append(' ');
        } else {
          first=false;
        }

        if (c.isProhibited()) {
          out.append('-');
        } else if (c.isRequired()) {
          out.append('+');
        }
        Query subQuery = c.getQuery();
        boolean wrapQuery=false;

        // TODO: may need to put parens around other types
        // of queries too, depending on future syntax.
        if (subQuery instanceof BooleanQuery) {
          wrapQuery=true;
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
      if (q.getMinimumNumberShouldMatch()>0) {
        out.append('~');
        out.append(Integer.toString(q.getMinimumNumberShouldMatch()));
      }

    } else if (query instanceof PrefixQuery) {
      PrefixQuery q = (PrefixQuery)query;
      Term prefix = q.getPrefix();
      FieldType ft = writeFieldName(prefix.field(), schema, out, flags);
      out.append(prefix.text());
      out.append('*');
    } else if (query instanceof ConstantScorePrefixQuery) {
      ConstantScorePrefixQuery q = (ConstantScorePrefixQuery)query;
      Term prefix = q.getPrefix();
      FieldType ft = writeFieldName(prefix.field(), schema, out, flags);
      out.append(prefix.text());
      out.append('*');
    } else if (query instanceof WildcardQuery) {
      out.append(query.toString());
      writeBoost=false;
    } else if (query instanceof FuzzyQuery) {
      out.append(query.toString());
      writeBoost=false;      
    } else if (query instanceof ConstantScoreQuery) {
      out.append(query.toString());
      writeBoost=false;
    } else {
      out.append(query.getClass().getSimpleName()
              + '(' + query.toString() + ')' );
      writeBoost=false;
    }

    if (writeBoost && query.getBoost() != 1.0f) {
      out.append("^");
      out.append(Float.toString(query.getBoost()));
    }

  }
  
  /**
   * Formats a Query for debugging, using the IndexSchema to make 
   * complex field types readable.
   *
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
      this(val,0,val.length());
    }

    public StrParser(String val, int start, int end) {
      this.val = val;
      this.pos = start;
      this.end = end;
    }

    void eatws() {
      while (pos<end && Character.isWhitespace(val.charAt(pos))) pos++;
    }

    void skip(int nChars) {
      pos = Math.max(pos+nChars, end);
    }

    boolean opt(String s) {
      eatws();
      int slen=s.length();
      if (val.regionMatches(pos, s, 0, slen)) {
        pos+=slen;
        return true;
      }
      return false;
    }

    boolean opt(char ch) {
      eatws();
      if (val.charAt(pos) == ch) {
        pos++;
        return true;
      }
      return false;
    }


    void expect(String s) throws ParseException {
      eatws();
      int slen=s.length();
      if (val.regionMatches(pos, s, 0, slen)) {
        pos+=slen;
      } else {
        throw new ParseException("Expected '"+s+"' at position " + pos + " in '"+val+"'");
      }
    }

    float getFloat() throws ParseException {
      eatws();
      char[] arr = new char[end-pos];
      int i;
      for (i=0; i<arr.length; i++) {
        char ch = val.charAt(pos);
        if ( (ch>='0' && ch<='9')
             || ch=='+' || ch=='-'
             || ch=='.' || ch=='e' || ch=='E'
        ) {
          pos++;
          arr[i]=ch;
        } else {
          break;
        }
      }

      return Float.parseFloat(new String(arr,0,i));
    }

    String getId() throws ParseException {
      eatws();
      int id_start=pos;
      if (pos<end && Character.isJavaIdentifierStart(val.charAt(pos))) {
        pos++;
        while (pos<end) {
          char ch = val.charAt(pos);
          if (!Character.isJavaIdentifierPart(ch) && ch!='.') {
            break;
          }
          pos++;
        }
        return val.substring(id_start, pos);
      }
      throw new ParseException("Expected identifier at pos " + pos + " str='" + val + "'");
    }

    // return null if not a string
    String getQuotedString() throws ParseException {
      eatws();
      char delim = peekChar();
      if (!(delim=='\"' || delim=='\'')) {
        return null;
      }
      int val_start = ++pos;
      StringBuilder sb = new StringBuilder(); // needed for escaping
      for(;;) {
        if (pos>=end) {
          throw new ParseException("Missing end quote for string at pos " + (val_start-1) + " str='"+val+"'");
        }
        char ch = val.charAt(pos);
        if (ch=='\\') {
          ch = pos<end ? val.charAt(pos++) : 0;
        } else if (ch==delim) {
          pos++;
          return sb.toString();
        }
        sb.append(ch);
        pos++;
      }
    }

    // next non-whitespace char
    char peek() {
      eatws();
      return pos<end ? val.charAt(pos) : 0;
    }

    // next char
    char peekChar() {
      return pos<end ? val.charAt(pos) : 0;  
    }

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

  /** 
   * Parse a function, returning a FunctionQuery
   *
   * <p>
   * Syntax Examples....
   * </p>
   *
   * <pre>
   * // Numeric fields default to correct type
   * // (ie: IntFieldSource or FloatFieldSource)
   * // Others use explicit ord(...) to generate numeric field value
   * myfield
   *
   * // OrdFieldSource
   * ord(myfield)
   *
   * // ReverseOrdFieldSource
   * rord(myfield)
   *
   * // LinearFloatFunction on numeric field value
   * linear(myfield,1,2)
   *
   * // MaxFloatFunction of LinearFloatFunction on numeric field value or constant
   * max(linear(myfield,1,2),100)
   *
   * // ReciprocalFloatFunction on numeric field value
   * recip(myfield,1,2,3)
   *
   * // ReciprocalFloatFunction on ReverseOrdFieldSource
   * recip(rord(myfield),1,2,3)
   *
   * // ReciprocalFloatFunction on LinearFloatFunction on ReverseOrdFieldSource
   * recip(linear(rord(myfield),1,2),3,4,5)
   * </pre>
   */
  public static FunctionQuery parseFunction(String func, IndexSchema schema) throws ParseException {
    SolrCore core = SolrCore.getSolrCore();
    return (FunctionQuery)(QParser.getParser(func,"func",new LocalSolrQueryRequest(core,new HashMap())).parse());
    // return new FunctionQuery(parseValSource(new StrParser(func), schema));
  }

}
