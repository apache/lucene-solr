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
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LegacyIntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

class CoreParserTestIndexData implements Closeable {

  final Directory dir;
  final IndexReader reader;
  final IndexSearcher searcher;

  CoreParserTestIndexData(Analyzer analyzer) throws Exception {
    BufferedReader d = new BufferedReader(new InputStreamReader(
        TestCoreParser.class.getResourceAsStream("reuters21578.txt"), StandardCharsets.US_ASCII));
    dir = LuceneTestCase.newDirectory();
    IndexWriter writer = new IndexWriter(dir, LuceneTestCase.newIndexWriterConfig(analyzer));
    String line = d.readLine();
    while (line != null) {
      int endOfDate = line.indexOf('\t');
      String date = line.substring(0, endOfDate).trim();
      String content = line.substring(endOfDate).trim();
      Document doc = new Document();
      doc.add(LuceneTestCase.newTextField("date", date, Field.Store.YES));
      doc.add(LuceneTestCase.newTextField("contents", content, Field.Store.YES));
      doc.add(new LegacyIntField("date2", Integer.parseInt(date), Field.Store.NO));
      doc.add(new IntPoint("date3", Integer.parseInt(date)));
      writer.addDocument(doc);
      line = d.readLine();
    }
    d.close();
    writer.close();
    reader = DirectoryReader.open(dir);
    searcher = LuceneTestCase.newSearcher(reader, false);
  }

  @Override
  public void close() throws IOException {
    reader.close();
    dir.close();
  }

}

