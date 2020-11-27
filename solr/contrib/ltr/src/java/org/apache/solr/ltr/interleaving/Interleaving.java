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

package org.apache.solr.ltr.interleaving;

import org.apache.lucene.search.ScoreDoc;
import org.apache.solr.common.SolrException;
import org.apache.solr.ltr.interleaving.algorithms.TeamDraftInterleaving;

/**
 * Interleaving considers two ranking models: modelA and modelB.
 * For a given query, each model returns its ranked list of documents La = (a1,a2,...) and Lb = (b1, b2, ...).
 * An Interleaving algorithm creates a unique ranked list I = (i1, i2, ...).
 * This list is created by interleaving elements from the two lists la and lb as described by the implementation algorithm.
 * Each element Ij is labelled TeamA if it is selected from La and TeamB if it is selected from Lb.
 */
public interface Interleaving {

   String TEAM_DRAFT = "TeamDraft";

   InterleavingResult interleave(ScoreDoc[] rerankedA, ScoreDoc[] rerankedB);

   static Interleaving getImplementation(String algorithm) {
      switch(algorithm) {
         case TEAM_DRAFT:
            return new TeamDraftInterleaving();
         default:
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Unknown Interleaving algorithm: " + algorithm);
      }
   }
}