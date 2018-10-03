package org.apache.yipeng;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Created by yipeng on 2018/10/1.
 */
public class TestIndexSearch {
  public static void main(String[] args) throws IOException {
    Directory directory = FSDirectory.open(TestIndexStructure.indexPath);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(LongPoint.newExactQuery("brand_id", 81l), BooleanClause.Occur.MUST);

    SortField sortField = new SortField("s1",SortField.Type.FLOAT,true);
    Sort sort = new Sort(sortField);
    TopFieldDocs topFieldDocs = searcher.search(builder.build(), 100, sort);

    for (ScoreDoc sc : topFieldDocs.scoreDocs) {
      Document document = reader.document(sc.doc);
      System.out.println(document.get("ware_id") + " , " + document.get("title") + " , " + document.get("s1"));
    }


  }
}
