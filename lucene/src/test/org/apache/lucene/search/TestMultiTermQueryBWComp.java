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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test MultiTermQuery api backwards compat
 * @deprecated Remove test when old API is no longer supported
 */
@Deprecated
public class TestMultiTermQueryBWComp extends LuceneTestCaseJ4 {
  private static RAMDirectory dir;
  private static Searcher searcher;
  private static final String FIELD = "test";
  
  /**
   * Test that the correct method (getTermsEnum/getEnum) is called.
   */
  @Test
  public void testEnumMethod() throws IOException {
    assertAPI("old", new OldAPI(FIELD));
    assertAPI("new", new NewAPI(FIELD));
    assertAPI("new", new BothAPI(FIELD));
    
    assertAPI("old2", new OldExtendsOldAPI(FIELD));
    assertAPI("old2", new OldExtendsNewAPI(FIELD));
    assertAPI("old2", new OldExtendsBothAPI(FIELD));
    
    assertAPI("new2", new NewExtendsOldAPI(FIELD));
    assertAPI("new2", new NewExtendsNewAPI(FIELD));
    assertAPI("new2", new NewExtendsBothAPI(FIELD));
    
    assertAPI("new2", new BothExtendsOldAPI(FIELD));
    assertAPI("new2", new BothExtendsNewAPI(FIELD));
    assertAPI("new2", new BothExtendsBothAPI(FIELD));
  }
  
  private static void assertAPI(String expected, Query query) throws IOException {
    TopDocs td = searcher.search(query, 25);
    assertEquals(1, td.totalHits);
    Document doc = searcher.doc(td.scoreDocs[0].doc);
    assertEquals(expected, doc.get(FIELD));
  }
  
  private class OldAPI extends MultiTermQuery {
    OldAPI(String field) { super(field); }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old"));
    }
    
    @Override
    public String toString(String field) { return null; }    
  }
  
  private class NewAPI extends MultiTermQuery {
    NewAPI(String field) { super(field); }
    
    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new"));
    }
    
    @Override
    public String toString(String field) { return null; }    
  }
  
  private class BothAPI extends MultiTermQuery {
    BothAPI(String field) { super(field); }
    
    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new"));
    }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old"));
    }
    
    @Override
    public String toString(String field) { return null; }    
  }
  
  private class OldExtendsOldAPI extends OldAPI {
    OldExtendsOldAPI(String field) { super(field); }

    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old2"));
    }
  }
  
  private class OldExtendsNewAPI extends NewAPI {
    OldExtendsNewAPI(String field) { super(field); }

    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old2"));
    }
  }
  
  private class OldExtendsBothAPI extends BothAPI {
    OldExtendsBothAPI(String field) { super(field); }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old2"));
    }
  }
  
  private class NewExtendsOldAPI extends OldAPI {
    NewExtendsOldAPI(String field) { super(field); }
    
    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new2"));
    }   
  }
  
  private class NewExtendsNewAPI extends NewAPI {
    NewExtendsNewAPI(String field) { super(field); }
 
    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new2"));
    }   
  }
  
  private class NewExtendsBothAPI extends BothAPI {
    NewExtendsBothAPI(String field) { super(field); }

    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new2"));
    }   
  }
  
  private class BothExtendsOldAPI extends OldAPI {
    BothExtendsOldAPI(String field) { super(field); }

    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new2"));
    }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old2"));
    } 
  }
  
  private class BothExtendsNewAPI extends NewAPI {
    BothExtendsNewAPI(String field) { super(field); }

    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new2"));
    }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old2"));
    } 
  }
  
  private class BothExtendsBothAPI extends BothAPI {
    BothExtendsBothAPI(String field) { super(field); }

    @Override
    protected TermsEnum getTermsEnum(IndexReader reader) throws IOException {
      return new SingleTermsEnum(reader, new Term(FIELD, "new2"));
    }
    
    @Override
    protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
      return new SingleTermEnum(reader, new Term(FIELD, "old2"));
    } 
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, 
        new MockAnalyzer(), true, 
        IndexWriter.MaxFieldLength.LIMITED);
    
    String values[] = { "old", "old2", "new", "new2" };
    for (String value : values) {
      Document doc = new Document();
      doc.add(new Field(FIELD, value, 
          Field.Store.YES, Field.Index.ANALYZED));   
      writer.addDocument(doc);
    }

    writer.optimize();
    writer.close();
    searcher = new IndexSearcher(dir, true);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher.close();
    searcher = null;
    dir.close();
    dir = null;
  }
}
