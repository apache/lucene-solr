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
package org.apache.solr.search;

import org.apache.lucene.index.Term;
import org.apache.solr.legacy.LegacyNumericRangeQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Collection of static utilities useful for query parsing.
 *
 *
 */
public class QueryParsing {
  public static final String OP = "q.op";  // the SolrParam used to override the QueryParser "default operator"
  public static final String V = "v";      // value of this parameter
  public static final String F = "f";      // field that a query or command pertains to
  public static final String TYPE = "type";// parser for this query or command
  public static final String DEFTYPE = "defType"; // default parser for any direct subqueries
  public static final String SPLIT_ON_WHITESPACE = "sow"; // Whether to split on whitespace prior to analysis
  public static final String LOCALPARAM_START = "{!";
  public static final char LOCALPARAM_END = '}';
  // true if the value was specified by the "v" param (i.e. v=myval, or v=$param)
  public static final String VAL_EXPLICIT = "__VAL_EXPLICIT__";

  /**
   * @param txt Text to parse
   * @param start Index into text for start of parsing
   * @param target Object to inject with parsed settings
   * @param params Additional existing parameters
   */
  public static int parseLocalParams(String txt, int start, ModifiableSolrParams target, SolrParams params) throws SyntaxError {
    return parseLocalParams(txt, start, target, params, LOCALPARAM_START, LOCALPARAM_END);
  }

  /**
   * @param txt Text to parse
   * @param start Index into text for start of parsing
   * @param target Object to inject with parsed settings
   * @param params Additional existing parameters
   * @param startString String that indicates the start of a localParams section
   * @param endChar Character that indicates the end of a localParams section
   */
  public static int parseLocalParams(String txt, int start, ModifiableSolrParams target, SolrParams params, String startString, char endChar) throws SyntaxError {
    int off = start;
    if (!txt.startsWith(startString, off)) return start;
    StrParser p = new StrParser(txt, start, txt.length());
    p.pos += startString.length(); // skip over "{!"

    for (; ;) {
      /*
      if (p.pos>=txt.length()) {
        throw new SyntaxError("Missing '}' parsing local params '" + txt + '"');
      }
      */
      char ch = p.peek();
      if (ch == endChar) {
        return p.pos + 1;
      }

      String id = p.getId();
      if (id.length() == 0) {
        throw new SyntaxError("Expected ending character '" + endChar + "' parsing local params '" + txt + '"');

      }
      String[] val = new String[1];

      ch = p.peek();
      if (ch != '=') {
        // single word... treat {!func} as type=func for easy lookup
        val[0] = id;
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
          val[0] = p.getQuotedString();
        } else {
          // read unquoted literal ended by whitespace or endChar (normally '}')
          // there is no escaping.
          int valStart = p.pos;
          for (; ;) {
            if (p.pos >= p.end) {
              throw new SyntaxError("Missing end to unquoted value starting at " + valStart + " str='" + txt + "'");
            }
            char c = p.val.charAt(p.pos);
            if (c == endChar || Character.isWhitespace(c)) {
              val[0] = p.val.substring(valStart, p.pos);
              break;
            }
            p.pos++;
          }
        }

        if (deref) {  // dereference parameter
          if (params != null) {
            val = params.getParams(val[0]);
          }
        }
      }
      if (target != null) target.add(id, val);
    }
  }


  /**
   * "foo" returns null
   * "{!prefix f=myfield}yes" returns type="prefix",f="myfield",v="yes"
   * "{!prefix f=myfield v=$p}" returns type="prefix",f="myfield",v=params.get("p")
   */
  public static SolrParams getLocalParams(String txt, SolrParams params) throws SyntaxError {
    if (txt == null || !txt.startsWith(LOCALPARAM_START)) {
      return null;
    }
    ModifiableSolrParams localParams = new ModifiableSolrParams();
    int start = QueryParsing.parseLocalParams(txt, 0, localParams, params);

    String val = localParams.get(V);
    if (val == null) {
      val = txt.substring(start);
      localParams.set(V, val);
    } else {
      // localParams.put(VAL_EXPLICIT, "true");
    }
    return localParams;
  }



  ///////////////////////////
  ///////////////////////////
  ///////////////////////////

  static FieldType writeFieldName(String name, IndexSchema schema, Appendable out, int flags) throws IOException {
    FieldType ft = null;
    ft = schema.getFieldTypeNoEx(name);
    out.append(name);
    if (ft == null) {
      out.append("(UNKNOWN FIELD ").append(name).append(String.valueOf(')'));
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
        CharsRefBuilder readable = new CharsRefBuilder();
        ft.indexedToReadable(val, readable);
        out.append(readable.get());
      } catch (Exception e) {
        out.append("EXCEPTION(val=");
        out.append(val.utf8ToString());
        out.append(")");
      }
    } else {
      out.append(val.utf8ToString());
    }
  }


  private static int FLAG_BOOSTED=0x01;
  private static int FLAG_IS_CLAUSE=0x02;
  /**
   * @see #toString(Query,IndexSchema)
   */
  public static void toString(Query query, IndexSchema schema, Appendable out, int flags) throws IOException {
    int subflag = flags & ~(FLAG_BOOSTED|FLAG_IS_CLAUSE);  // clear the boosted / is clause flags for recursion

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
    } else if (query instanceof LegacyNumericRangeQuery) {
      @SuppressWarnings({"rawtypes"})
      LegacyNumericRangeQuery q = (LegacyNumericRangeQuery) query;
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

      if (q.getMinimumNumberShouldMatch() != 0 || (flags & (FLAG_IS_CLAUSE | FLAG_BOOSTED)) != 0 ) {
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

        toString(subQuery, schema, out, subflag | FLAG_IS_CLAUSE);

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
    } else if (query instanceof FuzzyQuery) {
      out.append(query.toString());
    } else if (query instanceof ConstantScoreQuery) {
      out.append(query.toString());
    } else if (query instanceof WrappedQuery) {
      WrappedQuery q = (WrappedQuery)query;
      out.append(q.getOptions());
      toString(q.getWrappedQuery(), schema, out, subflag);
    } else if (query instanceof BoostQuery) {
      BoostQuery q = (BoostQuery)query;
      toString(q.getQuery(), schema, out, subflag | FLAG_BOOSTED);
      out.append('^');
      out.append(Float.toString(q.getBoost()));
    }
    else {
      out.append(query.getClass().getSimpleName()).append('(').append(query.toString()).append(')');
    }
  }

  /**
   * Formats a Query for debugging, using the IndexSchema to make
   * complex field types readable.
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
   * Builds a list of String which are stringified versions of a list of Queries
   */
  public static List<String> toString(List<Query> queries, IndexSchema schema) {
    List<String> out = new ArrayList<>(queries.size());
    for (Query q : queries) {
      out.add(QueryParsing.toString(q, schema));
    }
    return out;
  }

  /**
   * Parses default operator string into Operator object
   * @param operator the string from request
   * @return Operator.AND if string equals "AND", else return Operator.OR (default)
   */
  public static QueryParser.Operator parseOP(String operator) {
    return "and".equalsIgnoreCase(operator) ? QueryParser.Operator.AND : QueryParser.Operator.OR;
  }
}
