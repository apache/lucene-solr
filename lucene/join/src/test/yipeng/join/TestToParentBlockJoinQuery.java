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

package yipeng.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.store.RAMDirectory;

public class TestToParentBlockJoinQuery {
    public static void main(String[] args) throws IOException {

      CData cData2 = new CData(2, 1, "b", 199);

      CData cData3 = new CData(3, 1, "cdata3", 159);

      FData fData1 = new FData(1, "a", "1");

      CData cData1 = new CData(1, 2, "b", 88);

      FData fData2 = new FData(2, "f num b", "2");




      RAMDirectory directory = new RAMDirectory();
      IndexWriterConfig config = new IndexWriterConfig();
      IndexWriter writer = new IndexWriter(directory, config);
      List<Document> documents = new ArrayList<Document>();
      documents.add(createCDocument(cData2));
      documents.add(createCDocument(cData3));
      documents.add(createFDocument(fData1));
      writer.addDocuments(documents);
      documents.clear();
      documents.add(createCDocument(cData1));
      documents.add(createFDocument(fData2));
      writer.addDocuments(documents);


      TermQuery childQuery = new TermQuery(new Term("name", "a"));

      BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("doctype", "f")));
      ToParentBlockJoinQuery parentJoinQuery = new ToParentBlockJoinQuery(childQuery, parentsFilter, ScoreMode.Max);

      IndexReader reader = DirectoryReader.open(writer);
      IndexSearcher joinS = new IndexSearcher(reader);

      TopDocs joinedResults = joinS.search(parentJoinQuery, 2);
      SortedMap<Integer, TopDocs> joinResults = new TreeMap<>();
      for (ScoreDoc parentHit : joinedResults.scoreDocs) {
        ParentChildrenBlockJoinQuery childrenQuery = new ParentChildrenBlockJoinQuery(parentsFilter, childQuery, parentHit.doc);
        TopDocs childTopDocs = joinS.search(childrenQuery, 2);
        final Document parentDoc = joinS.doc(parentHit.doc);
        joinResults.put(Integer.valueOf(parentDoc.get("fid")), childTopDocs);
      }
      System.out.println(joinResults);
    }


  public static Document createFDocument(FData fData) {
    Document document = new Document();
    document.add(new StringField("fid", String.valueOf(fData.getfId()), Field.Store.YES));
    document.add(new StringField("name", fData.getfName(), Field.Store.YES));
    document.add(new StringField("doctype", "f", Field.Store.YES));
    return document;
  }

  public static Document createCDocument(CData cData) {
    Document document = new Document();
    document.add(new StringField("cid", String.valueOf(cData.getcId()), Field.Store.YES));
    document.add(new StringField("name", cData.getcName(), Field.Store.YES));
    document.add(new DoublePoint("price", cData.getPrice()));
    return document;
  }
}
