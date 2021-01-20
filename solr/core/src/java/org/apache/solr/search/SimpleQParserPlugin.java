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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SimpleParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
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
 *     q.operators - Used to enable specific operations for parsing.  The operations that can be enabled are
 *                   and, not, or, prefix, phrase, precedence, escape, and whitespace.  By default all operations
 *                   are enabled.  All operations can be disabled by passing in an empty string to this parameter.
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
  public static final String NAME = "simple";

  /** Map of string operators to their int counterparts in SimpleQueryParser. */
  private static final Map<String, Integer> OPERATORS = new HashMap<>();

  /* Setup the map of possible operators. */
  static {
    OPERATORS.put(SimpleParams.AND_OPERATOR,         SimpleQueryParser.AND_OPERATOR);
    OPERATORS.put(SimpleParams.NOT_OPERATOR,         SimpleQueryParser.NOT_OPERATOR);
    OPERATORS.put(SimpleParams.OR_OPERATOR,          SimpleQueryParser.OR_OPERATOR);
    OPERATORS.put(SimpleParams.PREFIX_OPERATOR,      SimpleQueryParser.PREFIX_OPERATOR);
    OPERATORS.put(SimpleParams.PHRASE_OPERATOR,      SimpleQueryParser.PHRASE_OPERATOR);
    OPERATORS.put(SimpleParams.PRECEDENCE_OPERATORS, SimpleQueryParser.PRECEDENCE_OPERATORS);
    OPERATORS.put(SimpleParams.ESCAPE_OPERATOR,      SimpleQueryParser.ESCAPE_OPERATOR);
    OPERATORS.put(SimpleParams.WHITESPACE_OPERATOR,  SimpleQueryParser.WHITESPACE_OPERATOR);
    OPERATORS.put(SimpleParams.FUZZY_OPERATOR,       SimpleQueryParser.FUZZY_OPERATOR);
    OPERATORS.put(SimpleParams.NEAR_OPERATOR,        SimpleQueryParser.NEAR_OPERATOR);
  }

  /** Returns a QParser that will create a query by using Lucene's SimpleQueryParser. */
  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new SimpleQParser(qstr, localParams, params, req);
  }

  private static class SimpleQParser extends QParser {
    private SimpleQueryParser parser;

    public SimpleQParser (String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {

      super(qstr, localParams, params, req);
      // Some of the parameters may come in through localParams, so combine them with params.
      SolrParams defaultParams = SolrParams.wrapDefaults(localParams, params);

      // This will be used to specify what fields and boosts will be used by SimpleQueryParser.
      Map<String, Float> queryFields = SolrPluginUtils.parseFieldBoosts(defaultParams.get(SimpleParams.QF));

      if (queryFields.isEmpty()) {
        // It qf is not specified setup up the queryFields map to use the defaultField.
        String defaultField = defaultParams.get(CommonParams.DF);

        if (defaultField == null) {
          // A query cannot be run without having a field or set of fields to run against.
          throw new IllegalStateException("Neither " + SimpleParams.QF + " nor " + CommonParams.DF
              + " are present.");
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
          Integer enabledOp = OPERATORS.get(operation.trim().toUpperCase(Locale.ROOT));

          if (enabledOp != null) {
            enabledOps |= enabledOp;
          }
        }
      }

      // Create a SimpleQueryParser using the analyzer from the schema.
      final IndexSchema schema = req.getSchema();
      parser = new SolrSimpleQueryParser(req.getSchema().getQueryAnalyzer(), queryFields, enabledOps, this, schema);

      // Set the default operator to be either 'AND' or 'OR' for the query.
      QueryParser.Operator defaultOp = QueryParsing.parseOP(defaultParams.get(QueryParsing.OP));

      if (defaultOp == QueryParser.Operator.AND) {
        parser.setDefaultOperator(BooleanClause.Occur.MUST);
      }
    }

    @Override
    public Query parse() throws SyntaxError {
      return parser.parse(qstr);
    }

  }

  private static class SolrSimpleQueryParser extends SimpleQueryParser {
    QParser qParser;
    IndexSchema schema;

    public SolrSimpleQueryParser(Analyzer analyzer, Map<String, Float> weights, int flags,
                                 QParser qParser, IndexSchema schema) {
      super(analyzer, weights, flags);
      this.qParser = qParser;
      this.schema = schema;
    }

    @Override
    protected Query newPrefixQuery(String text) {
      BooleanQuery.Builder bq = new BooleanQuery.Builder();

      for (Map.Entry<String, Float> entry : weights.entrySet()) {
        String field = entry.getKey();
        FieldType type = schema.getFieldType(field);
        Query prefix = null;

        if (type instanceof TextField) {
          // If the field type is a TextField then use the multi term analyzer.
          Analyzer analyzer = ((TextField)type).getMultiTermAnalyzer();
          BytesRef termBytes = TextField.analyzeMultiTerm(field, text, analyzer);
          if (termBytes != null) {
            String term = termBytes.utf8ToString();
            SchemaField sf = schema.getField(field);
            prefix = sf.getType().getPrefixQuery(qParser, sf, term);
          }
        } else {
          // If the type is *not* a TextField don't do any analysis.
          SchemaField sf = schema.getField(field);
          prefix = type.getPrefixQuery(qParser, sf, text);
        }
        if (prefix != null) {
          float boost = entry.getValue();
          if (boost != 1f) {
            prefix = new BoostQuery(prefix, boost);
          }
          bq.add(prefix, BooleanClause.Occur.SHOULD);
        }
      }

      return simplify(bq.build());
    }

    @Override
    protected Query newFuzzyQuery(String text, int fuzziness) {
      BooleanQuery.Builder bq = new BooleanQuery.Builder();

      for (Map.Entry<String, Float> entry : weights.entrySet()) {
        String field = entry.getKey();
        FieldType type = schema.getFieldType(field);
        Query fuzzy = null;

        if (type instanceof TextField) {
          // If the field type is a TextField then use the multi term analyzer.
          Analyzer analyzer = ((TextField)type).getMultiTermAnalyzer();
          BytesRef termBytes = TextField.analyzeMultiTerm(field, text, analyzer);
          if (termBytes != null) {
            String term = termBytes.utf8ToString();
            fuzzy = new FuzzyQuery(new Term(entry.getKey(), term), fuzziness);
          }
        } else {
          // If the type is *not* a TextField don't do any analysis.
          fuzzy = new FuzzyQuery(new Term(entry.getKey(), text), fuzziness);
        }
        if (fuzzy != null) {
          float boost = entry.getValue();
          if (boost != 1f) {
            fuzzy = new BoostQuery(fuzzy, boost);
          }
          bq.add(fuzzy, BooleanClause.Occur.SHOULD);
        }
      }

      return simplify(bq.build());
    }


  }
}

