package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.IOException; 
import java.util.ArrayList; 
import java.util.List; 
 
import junit.framework.TestCase; 
 
import org.apache.lucene.analysis.standard.StandardAnalyzer; 
import org.apache.lucene.document.Document; 
import org.apache.lucene.document.Field; 
import org.apache.lucene.gdata.storage.lucenestorage.ModifiedEntryFilter; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper; 
import org.apache.lucene.index.IndexReader; 
import org.apache.lucene.index.IndexWriter; 
import org.apache.lucene.index.Term; 
import org.apache.lucene.search.Hits; 
import org.apache.lucene.search.IndexSearcher; 
import org.apache.lucene.search.Query; 
import org.apache.lucene.search.Searcher; 
import org.apache.lucene.search.TermQuery; 
import org.apache.lucene.store.RAMDirectory; 
 
public class TestModifiedEntryFilter extends TestCase { 
    IndexWriter writer; 
    IndexReader reader; 
    List<String> excludeList; 
    String feedID = "feed"; 
    String fieldFeedId = "feedID"; 
    protected void setUp() throws Exception { 
        RAMDirectory dir = new RAMDirectory(); 
        this.writer = new IndexWriter(dir,new StandardAnalyzer(),true); 
        Document doc = new Document(); 
        doc.add(new Field(StorageEntryWrapper.FIELD_ENTRY_ID,"1",Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        doc.add(new Field(fieldFeedId,feedID,Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        Document doc1 = new Document(); 
        doc1.add(new Field(StorageEntryWrapper.FIELD_ENTRY_ID,"2",Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        doc1.add(new Field(fieldFeedId,feedID,Field.Store.YES,Field.Index.UN_TOKENIZED)); 
        this.writer.addDocument(doc); 
        this.writer.addDocument(doc1); 
        this.writer.close(); 
        this.reader = IndexReader.open(dir); 
        this.excludeList = new ArrayList(); 
        this.excludeList.add("1"); 
         
         
    } 
 
    protected void tearDown() throws Exception { 
        super.tearDown(); 
    } 
    public void testFilter() throws IOException{ 
        Searcher s = new IndexSearcher(this.reader); 
        Query q = new TermQuery(new Term(fieldFeedId,feedID)); 
        Hits hits = s.search(q); 
        assertEquals(2,hits.length()); 
         
        hits = s.search(q,new ModifiedEntryFilter(this.excludeList.toArray(new String[0]))); 
        assertEquals(1,hits.length()); 
        this.excludeList.add("2"); 
 
        hits = s.search(q,new ModifiedEntryFilter(this.excludeList.toArray(new String[0]))); 
        assertEquals(0,hits.length()); 
         
    } 
} 
