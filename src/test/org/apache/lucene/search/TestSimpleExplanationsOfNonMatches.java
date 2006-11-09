package org.apache.lucene.search;

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


import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import org.apache.lucene.analysis.WhitespaceAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;

import junit.framework.TestCase;

import java.util.Random;
import java.util.BitSet;

/**
 * subclass of TestSimpleExplanations that verifies non matches.
 */
public class TestSimpleExplanationsOfNonMatches
  extends TestSimpleExplanations {

  /**
   * Overrides superclass to ignore matches and focus on non-matches
   *
   * @see CheckHits#checkNoMatchExplanations
   */
  public void qtest(Query q, int[] expDocNrs) throws Exception {
    CheckHits.checkNoMatchExplanations(q, FIELD, searcher, expDocNrs);
  }
    
}
