package org.apache.solr.spelling;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpellCheckCollator {
  private static final Logger LOG = LoggerFactory.getLogger(SpellCheckCollator.class);

  public List<SpellCheckCollation> collate(SpellingResult result, String originalQuery, ResponseBuilder ultimateResponse,
                                           int maxCollations, int maxTries, int maxEvaluations) {
    List<SpellCheckCollation> collations = new ArrayList<SpellCheckCollation>();

    QueryComponent queryComponent = null;
    if (ultimateResponse.components != null) {
      for (SearchComponent sc : ultimateResponse.components) {
        if (sc instanceof QueryComponent) {
          queryComponent = (QueryComponent) sc;
          break;
        }
      }
    }

    boolean verifyCandidateWithQuery = true;
    if (maxTries < 1) {
      maxTries = 1;
      verifyCandidateWithQuery = false;
    }
    if (queryComponent == null && verifyCandidateWithQuery) {
      LOG.info("Could not find an instance of QueryComponent.  Disabling collation verification against the index.");
      maxTries = 1;
      verifyCandidateWithQuery = false;
    }

    int tryNo = 0;
    int collNo = 0;
    PossibilityIterator possibilityIter = new PossibilityIterator(result.getSuggestions(), maxTries, maxEvaluations);
    while (tryNo < maxTries && collNo < maxCollations && possibilityIter.hasNext()) {

      RankedSpellPossibility possibility = possibilityIter.next();
      String collationQueryStr = getCollation(originalQuery, possibility.getCorrections());
      int hits = 0;

      if (verifyCandidateWithQuery) {
        tryNo++;

        ModifiableSolrParams params = new ModifiableSolrParams(ultimateResponse.req.getParams());
        params.set(CommonParams.Q, collationQueryStr);
        params.remove(CommonParams.START);
        params.set(CommonParams.FL, "id");
        params.set(CommonParams.ROWS, "0");
        params.remove(GroupParams.GROUP);

        // creating a request here... make sure to close it!
        ResponseBuilder checkResponse = new ResponseBuilder(new LocalSolrQueryRequest(ultimateResponse.req.getCore(), params),new SolrQueryResponse(), Arrays.asList(new SearchComponent[] { queryComponent }));
        checkResponse.setQparser(ultimateResponse.getQparser());
        checkResponse.setFilters(ultimateResponse.getFilters());
        checkResponse.setQueryString(collationQueryStr);
        checkResponse.components = Arrays.asList(new SearchComponent[] { queryComponent });

        try {
          queryComponent.prepare(checkResponse);
          queryComponent.process(checkResponse);
          hits = (Integer) checkResponse.rsp.getToLog().get("hits");
        } catch (Exception e) {
          LOG.warn("Exception trying to re-query to check if a spell check possibility would return any hits.", e);
        } finally {
          checkResponse.req.close();  
        }
      }
      if (hits > 0 || !verifyCandidateWithQuery) {
        collNo++;
        SpellCheckCollation collation = new SpellCheckCollation();
        collation.setCollationQuery(collationQueryStr);
        collation.setHits(hits);
        collation.setInternalRank(possibility.getRank());

        NamedList<String> misspellingsAndCorrections = new NamedList<String>();
        for (SpellCheckCorrection corr : possibility.getCorrections()) {
          misspellingsAndCorrections.add(corr.getOriginal().toString(), corr.getCorrection());
        }
        collation.setMisspellingsAndCorrections(misspellingsAndCorrections);
        collations.add(collation);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Collation: " + collationQueryStr + (verifyCandidateWithQuery ? (" will return " + hits + " hits.") : ""));
      }
    }
    return collations;
  }

  private String getCollation(String origQuery,
                              List<SpellCheckCorrection> corrections) {
    StringBuilder collation = new StringBuilder(origQuery);
    int offset = 0;
    for (SpellCheckCorrection correction : corrections) {
      Token tok = correction.getOriginal();
      // we are replacing the query in order, but injected terms might cause
      // illegal offsets due to previous replacements.
      if (tok.getPositionIncrement() == 0)
        continue;
      collation.replace(tok.startOffset() + offset, tok.endOffset() + offset,
          correction.getCorrection());
      offset += correction.getCorrection().length()
          - (tok.endOffset() - tok.startOffset());
    }
    return collation.toString();
  }

}
