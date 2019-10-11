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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.DefaultStreamFactory;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds documents whose id's match values returned from a stream of tuples from a Streaming Expression.
 * The value passed to the parser must be a valid Streaming Expression that returns tuples, and one of the keys
 * in the tuples must match the uniqueId field in your schema (or otherwise the tuple key must be specified
 * through the <code>f</code> parameter.
 * <br>Other Parameters:
 * <br><code>f</code>: (optional) The field name from the streaming expression containing the document ids upon which to filter
 * <br><code>method</code>: (optional) Any of termsFilter (default), booleanQuery, automaton, docValuesTermsFilter.
 * <p>
 * Note that if no values are specified then the query matches no documents.
 */
public class StreamingExpressionQParserPlugin extends QParserPlugin {
  public static final String NAME = "streaming_expression";


  /** Choose the internal algorithm.  Remove this later if we have a more efficient way to join */
  private static final String METHOD = "method";

  static SolrClientCache clientCache = new SolrClientCache();
  private StreamFactory streamFactory = new DefaultStreamFactory();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());



  private enum Method {
    termsFilter {
      @Override
      Query makeFilter(String fname, BytesRef[] bytesRefs) {
        return new TermInSetQuery(fname, bytesRefs);// constant scores
      }
    },
    booleanQuery {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef byteRef : byteRefs) {
          bq.add(new TermQuery(new Term(fname, byteRef)), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(bq.build());
      }
    },
    automaton {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        ArrayUtil.timSort(byteRefs); // same sort algo as TermInSetQuery's choice
        Automaton union = Automata.makeStringUnion(Arrays.asList(byteRefs)); // input must be sorted
        return new AutomatonQuery(new Term(fname), union);//constant scores
      }
    },
    docValuesTermsFilter {//on 4x this is FieldCacheTermsFilter but we use the 5x name any way
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        return new DocValuesTermsQuery(fname, byteRefs);//constant scores
      }
    };

    abstract Query makeFilter(String fname, BytesRef[] byteRefs);
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {

      @Override
      public Query parse() {


        Method method = Method.valueOf(localParams.get(METHOD, Method.termsFilter.name()));
        //TODO pick the default method based on various heuristics from benchmarks
        //TODO pick the default using FieldType.getSetQuery
        //FieldType ft = req.getSchema().getFieldTypeNoEx(fname);


        CoreContainer coreContainer = req.getCore().getCoreContainer();
        SolrCore core = req.getCore();
        if (coreContainer.isZooKeeperAware()) {
          String defaultCollection = core.getCoreDescriptor().getCollectionName();
          String defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
          streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
          streamFactory.withDefaultZkHost(defaultZkhost);
        }

        TupleStream tupleStream = null;

        String expression = localParams.get(QueryParsing.V);
        if (StringUtils.isEmpty(expression)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Streaming Expression missing.");
        }

        try {
          tupleStream = streamFactory.constructStream(expression);
        } catch (Exception e) {
          //Catch exceptions that occur while the stream is being created. This will include streaming expression parse rules.
          SolrException.log(log, e);
        }

        StreamContext context = new StreamContext();
        context.setSolrClientCache(clientCache);
        context.put("core", core.getName());
        tupleStream.setStreamContext(context);
        Map requestContext = req.getContext();
        requestContext.put("stream", tupleStream);

        if (null ==  req.getSchema().getUniqueKeyField()){
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NAME + " can only be used on collection with a " +
              "uniqueKey field in their schema");
        }
        String idFieldName = req.getSchema().getUniqueKeyField().getName();


        FieldType idFieldType = req.getSchema().getFieldTypeNoEx(idFieldName);

        String tupleIdFieldName = localParams.get(QueryParsing.F, idFieldName);

        List<Tuple> tuples = new ArrayList();
        try {
          tupleStream.open();
          for (; ; ) {
            Tuple t = tupleStream.read();
            if (t.EOF) {
              break;
            } else {
              tuples.add(t);
            }
          }
        } catch (IOException e) {
        } finally {
          try {
            tupleStream.close();
          } catch (IOException e) {
          }
        }

        BytesRef[] bytesRefs = new BytesRef[tuples.size()];
        BytesRefBuilder term = new BytesRefBuilder();

        for (int i = 0; i < tuples.size(); i++) {
          String idValue = tuples.get(i).getString(idFieldName);
          if (idFieldType != null) {
            idFieldType.readableToIndexed(idValue, term);

          } else {
            term.copyChars(idValue);
          }

          bytesRefs[i] = term.toBytesRef();
        }

        return method.makeFilter(tupleIdFieldName, bytesRefs);
      }
    };
  }
}
