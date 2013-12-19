package org.apache.solr.search;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SimpleParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TextField;
import org.apache.solr.util.SolrPluginUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Create a query from the input value that will be parsed by Lucene's SimpleQueryParser.
 * See {@link org.apache.lucene.queryparser.simple.SimpleQueryParser} for details on the exact syntax allowed
 * to be used for queries.
 * <br>
 * The following options may be applied for parsing the query.
 * <ul>
 *   <li>
 *     q.operations - Used to enable specific operations for parsing.  The operations that can be enabled are
 *                    and, not, or, prefix, phrase, precedence, escape, and whitespace.  By default all operations
 *                    are enabled.  All operations can be disabled by passing in an empty string to this parameter.
 *   </li>
 *   <li>
 *     q.op - Used to specify the operator to be used if whitespace is a delimiter. Either 'AND' or 'OR'
 *            can be specified for this parameter.  Any other string will cause an exception to be thrown.
 *            If this parameter is not specified 'OR' will be used by default.
 *   </li>
 *   <li>
 *     qf - The list of query fields and boosts to use when building the simple query.  The format is the following:
 *          <code>fieldA^1.0 fieldB^2.2</code>.  A field can also be specified without a boost by simply listing the
 *          field as <code>fieldA fieldB</code>.  Any field without a boost will default to use a boost of 1.0.
 *   </li>
 *   <li>
 *     df - An override for the default field specified in the schema or a default field if one is not specified
 *          in the schema.  If qf is not specified the default field will be used as the field to run the query
 *          against.
 *   </li>
 * </ul>
 */
public class SimpleQParserPlugin extends QParserPlugin {
  /** The name that can be used to specify this plugin should be used to parse the query. */
  public static String NAME = "simple";

  /** Enables {@code AND} operator (+) */
  private static final String AND_OPERATOR         = "AND";
  /** Enables {@code NOT} operator (-) */
  private static final String NOT_OPERATOR         = "NOT";
  /** Enables {@code OR} operator (|) */
  private static final String OR_OPERATOR          = "OR";
  /** Enables {@code PREFIX} operator (*) */
  private static final String PREFIX_OPERATOR      = "PREFIX";
  /** Enables {@code PHRASE} operator (") */
  private static final String PHRASE_OPERATOR      = "PHRASE";
  /** Enables {@code PRECEDENCE} operators: {@code (} and {@code )} */
  private static final String PRECEDENCE_OPERATORS = "PRECEDENCE";
  /** Enables {@code ESCAPE} operator (\) */
  private static final String ESCAPE_OPERATOR      = "ESCAPE";
  /** Enables {@code WHITESPACE} operators: ' ' '\n' '\r' '\t' */
  private static final String WHITESPACE_OPERATOR  = "WHITESPACE";

  /** Map of string operators to their int counterparts in SimpleQueryParser. */
  private static final Map<String, Integer> OPERATORS = new HashMap<String, Integer>();

  /* Setup the map of possible operators. */
  static {
    OPERATORS.put(AND_OPERATOR,         SimpleQueryParser.AND_OPERATOR);
    OPERATORS.put(NOT_OPERATOR,         SimpleQueryParser.NOT_OPERATOR);
    OPERATORS.put(OR_OPERATOR,          SimpleQueryParser.OR_OPERATOR);
    OPERATORS.put(PREFIX_OPERATOR,      SimpleQueryParser.PREFIX_OPERATOR);
    OPERATORS.put(PHRASE_OPERATOR,      SimpleQueryParser.PHRASE_OPERATOR);
    OPERATORS.put(PRECEDENCE_OPERATORS, SimpleQueryParser.PRECEDENCE_OPERATORS);
    OPERATORS.put(ESCAPE_OPERATOR,      SimpleQueryParser.ESCAPE_OPERATOR);
    OPERATORS.put(WHITESPACE_OPERATOR,  SimpleQueryParser.WHITESPACE_OPERATOR);
  }

  /** No initialization is necessary so this method is empty. */
  @Override
  public void init(NamedList args) {
  }

  /** Returns a QParser that will create a query by using Lucene's SimpleQueryParser. */
  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    // Some of the parameters may come in through localParams, so combine them with params.
    SolrParams defaultParams = SolrParams.wrapDefaults(localParams, params);

    // This will be used to specify what fields and boosts will be used by SimpleQueryParser.
    Map<String, Float> queryFields = SolrPluginUtils.parseFieldBoosts(defaultParams.get(SimpleParams.QF));

    if (queryFields.isEmpty()) {
      // It qf is not specified setup up the queryFields map to use the defaultField.
      String defaultField = QueryParsing.getDefaultField(req.getSchema(), defaultParams.get(CommonParams.DF));

      if (defaultField == null) {
        // A query cannot be run without having a field or set of fields to run against.
        throw new IllegalStateException("Neither " + SimpleParams.QF + ", " + CommonParams.DF
            + ", nor the default search field are present.");
      }

      queryFields.put(defaultField, 1.0F);
    }
    else {
      for (Map.Entry<String, Float> queryField : queryFields.entrySet()) {
        if (queryField.getValue() == null) {
          // Some fields may be specified without a boost, so default the boost to 1.0 since a null value
          // will not be accepted by SimpleQueryParser.
          queryField.setValue(1.0F);
        }
      }
    }

    // Setup the operations that are enabled for the query.
    int enabledOps = 0;
    String opParam = defaultParams.get(SimpleParams.QO);

    if (opParam == null) {
      // All operations will be enabled.
      enabledOps = -1;
    } else {
      // Parse the specified enabled operations to be used by the query.
      String[] operations = opParam.split(",");

      for (String operation : operations) {
        Integer enabledOp = OPERATORS.get(operation.trim().toUpperCase(Locale.getDefault()));

        if (enabledOp != null) {
          enabledOps |= enabledOp;
        }
      }
    }

    // Create a SimpleQueryParser using the analyzer from the schema.
    final IndexSchema schema = req.getSchema();
    final SimpleQueryParser parser = new SimpleQueryParser(req.getSchema().getAnalyzer(), queryFields, enabledOps) {
      // Override newPrefixQuery to provide a multi term analyzer for prefix queries run against TextFields.
      @Override
      protected Query newPrefixQuery(String text) {
        BooleanQuery bq = new BooleanQuery(true);

        for (Map.Entry<String, Float> entry : weights.entrySet()) {
          String field = entry.getKey();
          FieldType type = schema.getFieldType(field);
          Query prefix;

          if (type instanceof TextField) {
            // If the field type is a TextField then use the multi term analyzer.
            Analyzer analyzer = ((TextField)type).getMultiTermAnalyzer();
            String term = TextField.analyzeMultiTerm(field, text, analyzer).utf8ToString();
            prefix = new PrefixQuery(new Term(field, term));
          } else {
            // If the type is *not* a TextField don't do any analysis.
            prefix = new PrefixQuery(new Term(entry.getKey(), text));
          }

          prefix.setBoost(entry.getValue());
          bq.add(prefix, BooleanClause.Occur.SHOULD);
        }

        return simplify(bq);
      }
    };

    // Set the default operator to be either 'AND' or 'OR' for the query.
    QueryParser.Operator defaultOp = QueryParsing.getQueryParserDefaultOperator(req.getSchema(), defaultParams.get(QueryParsing.OP));

    if (defaultOp == QueryParser.Operator.AND) {
      parser.setDefaultOperator(BooleanClause.Occur.MUST);
    }

    // Return a QParser that wraps a SimpleQueryParser.
    return new QParser(qstr, localParams, params, req) {
      public Query parse() throws SyntaxError {
        return parser.parse(qstr);
      }
    };
  }
}
