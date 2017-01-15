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

package org.apache.lucene.queries.function;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TopDocs;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFunctionMatchQuery extends FunctionTestSetup {

  static IndexReader reader;
  static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
  }

  public void testRangeMatching() throws IOException {
    DoubleValuesSource in = DoubleValuesSource.fromFloatField(FLOAT_FIELD);
    FunctionMatchQuery fmq = new FunctionMatchQuery(in, d -> d >= 2 && d < 4);
    TopDocs docs = searcher.search(fmq, 10);

    assertEquals(2, docs.totalHits);
    assertEquals(9, docs.scoreDocs[0].doc);
    assertEquals(13, docs.scoreDocs[1].doc);

    QueryUtils.check(random(), fmq, searcher, rarely());

  }

}
