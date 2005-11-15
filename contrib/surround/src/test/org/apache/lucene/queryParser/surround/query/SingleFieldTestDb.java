package org.apache.lucene.queryParser.surround.query;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;

public class SingleFieldTestDb {
  private Directory db;
  private String[] docs;
  private String fieldName;
  
  public SingleFieldTestDb(String[] documents, String fName) {
    try {
      db = new RAMDirectory();
      docs = documents;
      fieldName = fName;
      Analyzer analyzer = new WhitespaceAnalyzer();
      IndexWriter writer = new IndexWriter(db, analyzer, true);
      for (int j = 0; j < docs.length; j++) {
        Document d = new Document();
        d.add(new Field(fieldName, docs[j], Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(d);
      }
      writer.close();
    } catch (java.io.IOException ioe) {
      throw new Error(ioe);
    }
  }
  
  Directory getDb() {return db;}
  String[] getDocs() {return docs;}
  String getFieldname() {return fieldName;}
}

