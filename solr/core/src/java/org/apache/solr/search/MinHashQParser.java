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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * The query parser can be used in two modes
 * 1) where text is analysed and generates min hashes as part of normal lucene analysis
 * 2) where text is pre-analysed and hashes are added as string to the index
 *    An analyzer can still be defined to support text based query against the text field
 * <p>
 * Options:
 * sim - required similary - default is 1
 * tp - required true positive rate - default is 1
 * field - when providing text the analyser for this field is used to generate the finger print
 * sep - a separator for provided hashes
 * analyzer_field - the field to use for for analysing suppplied text - if not supplied defaults to field
 *
 */
public class MinHashQParser extends QParser {
  public MinHashQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {

    float similarity = localParams.getFloat("sim", 1.0f);
    float expectedTruePositive = localParams.getFloat("tp", 1.0f);
    String field = localParams.get("field", "min_hash");
    String separator = localParams.get("sep", "");
    String analyzerField = localParams.get("analyzer_field", field);


    ArrayList<BytesRef> hashes = new ArrayList<>();
    if (separator.isEmpty()) {
      try {
        getHashesFromTokenStream(analyzerField, hashes);
      } catch (Exception e) {
        throw new SyntaxError(e);
      }
    } else {
      getHashesFromQueryString(separator, hashes);
    }

    return createFingerPrintQuery(field, hashes, similarity, expectedTruePositive);

  }

  private void getHashesFromQueryString(String separator, ArrayList<BytesRef> hashes) {
    Arrays.stream(qstr.split(separator)).forEach(s -> {
      hashes.add(new BytesRef(s));
    });
  }

  private void getHashesFromTokenStream(String analyserField, ArrayList<BytesRef> hashes) throws Exception {
    TokenStream ts = getReq().getSchema().getIndexAnalyzer().tokenStream(analyserField, qstr);
    TermToBytesRefAttribute termAttribute = ts.getAttribute(TermToBytesRefAttribute.class);
    ts.reset();
    while (ts.incrementToken()) {
      BytesRef term = termAttribute.getBytesRef();
      hashes.add(BytesRef.deepCopyOf(term));
    }
    ts.end();
    ts.close();
  }

  private Query createFingerPrintQuery(String field, List<BytesRef> minhashes, float similarity, float expectedTruePositive) {
    int bandSize = 1;
    if (expectedTruePositive < 1) {
      bandSize = computeBandSize(minhashes.size(), similarity, expectedTruePositive);
    }

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    BooleanQuery.Builder childBuilder = new BooleanQuery.Builder();
    int rowInBand = 0;
    for (BytesRef minHash : minhashes) {
      TermQuery tq = new TermQuery(new Term(field, minHash));
      if (bandSize == 1) {
        builder.add(new ConstantScoreQuery(tq), Occur.SHOULD);
      } else {
        childBuilder.add(new ConstantScoreQuery(tq), Occur.MUST);
        rowInBand++;
        if (rowInBand == bandSize) {
          builder.add(new ConstantScoreQuery(childBuilder.build()),
              Occur.SHOULD);
          childBuilder = new BooleanQuery.Builder();
          rowInBand = 0;
        }
      }
    }
    // Avoid a dubious narrow band .... wrap around and pad with the
    // start
    if (childBuilder.build().clauses().size() > 0) {
      for (BytesRef token : minhashes) {
        TermQuery tq = new TermQuery(new Term(field, token.toString()));
        childBuilder.add(new ConstantScoreQuery(tq), Occur.MUST);
        rowInBand++;
        if (rowInBand == bandSize) {
          builder.add(new ConstantScoreQuery(childBuilder.build()),
              Occur.SHOULD);
          break;
        }
      }
    }

    if (expectedTruePositive >= 1.0 && similarity < 1) {
      builder.setMinimumNumberShouldMatch((int) (Math.ceil(minhashes.size() * similarity)));
    }
    return builder.build();

  }

  static int computeBandSize(int numHash, double similarity, double expectedTruePositive) {
    for (int bands = 1; bands <= numHash; bands++) {
      int rowsInBand = numHash / bands;
      double truePositive = 1 - Math.pow(1 - Math.pow(similarity, rowsInBand), bands);
      if (truePositive > expectedTruePositive) {
        return rowsInBand;
      }
    }
    return 1;
  }
}
