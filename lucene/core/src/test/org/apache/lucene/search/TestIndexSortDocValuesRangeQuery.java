package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IndexSortDocValuesRangeQuery;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

@Repeat(iterations = 100)
public class TestIndexSortDocValuesRangeQuery extends LuceneTestCase {

  public void testDocValuesWithEvenLength() throws Exception {
    testDocValuesWithEvenLength(false);
    testDocValuesWithEvenLength(true);
  }

  public void testDocValuesWithEvenLength(boolean reverse) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    IndexWriter writer = new IndexWriter(dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 30));
    writer.addDocument(createDocument("field", 35));

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertEquals(1, searcher.count(createQuery("field", -80, -80)));
    assertEquals(1, searcher.count(createQuery("field", -5, -5)));
    assertEquals(2, searcher.count(createQuery("field", 0, 0)));
    assertEquals(1, searcher.count(createQuery("field", 30, 30)));
    assertEquals(1, searcher.count(createQuery("field", 35, 35)));

    assertEquals(0, searcher.count(createQuery("field", -90, -90)));
    assertEquals(0, searcher.count(createQuery("field", 5, 5)));
    assertEquals(0, searcher.count(createQuery("field", 40, 40)));

    // Test the lower end of the document value range.
    assertEquals(2, searcher.count(createQuery("field", -90, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -4)));
    assertEquals(1, searcher.count(createQuery("field", -70, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -5)));

    // Test the upper end of the document value range.
    assertEquals(1, searcher.count(createQuery("field", 25, 34)));
    assertEquals(2, searcher.count(createQuery("field", 25, 35)));
    assertEquals(2, searcher.count(createQuery("field", 25, 36)));
    assertEquals(2, searcher.count(createQuery("field", 30, 35)));

    // Test multiple occurrences of the same value.
    assertEquals(2, searcher.count(createQuery("field", -4, 4)));
    assertEquals(2, searcher.count(createQuery("field", -4, 0)));
    assertEquals(2, searcher.count(createQuery("field", 0, 4)));
    assertEquals(3, searcher.count(createQuery("field", 0, 30)));

    // Test ranges that span all documents.
    assertEquals(6, searcher.count(createQuery("field", -80, 35)));
    assertEquals(6, searcher.count(createQuery("field", -90, 40)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testDocValuesWithOddLength() throws Exception {
    testDocValuesWithOddLength(false);
    testDocValuesWithOddLength(true);
  }

  public void testDocValuesWithOddLength(boolean reverse) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    IndexWriter writer = new IndexWriter(dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 5));
    writer.addDocument(createDocument("field", 30));
    writer.addDocument(createDocument("field", 35));

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertEquals(1, searcher.count(createQuery("field", -80, -80)));
    assertEquals(1, searcher.count(createQuery("field", -5, -5)));
    assertEquals(2, searcher.count(createQuery("field", 0, 0)));
    assertEquals(1, searcher.count(createQuery("field", 5, 5)));
    assertEquals(1, searcher.count(createQuery("field", 30, 30)));
    assertEquals(1, searcher.count(createQuery("field", 35, 35)));

    assertEquals(0, searcher.count(createQuery("field", -90, -90)));
    assertEquals(0, searcher.count(createQuery("field", 6, 6)));
    assertEquals(0, searcher.count(createQuery("field", 40, 40)));

    // Test the lower end of the document value range.
    assertEquals(2, searcher.count(createQuery("field", -90, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -4)));
    assertEquals(1, searcher.count(createQuery("field", -70, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -5)));

    // Test the upper end of the document value range.
    assertEquals(1, searcher.count(createQuery("field", 25, 34)));
    assertEquals(2, searcher.count(createQuery("field", 25, 35)));
    assertEquals(2, searcher.count(createQuery("field", 25, 36)));
    assertEquals(2, searcher.count(createQuery("field", 30, 35)));

    // Test multiple occurrences of the same value.
    assertEquals(2, searcher.count(createQuery("field", -4, 4)));
    assertEquals(2, searcher.count(createQuery("field", -4, 0)));
    assertEquals(2, searcher.count(createQuery("field", 0, 4)));
    assertEquals(4, searcher.count(createQuery("field", 0, 30)));

    // Test ranges that span all documents.
    assertEquals(7, searcher.count(createQuery("field", -80, 35)));
    assertEquals(7, searcher.count(createQuery("field", -90, 40)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testMinAndMaxLongValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter writer = new IndexWriter(dir, iwc);

    writer.addDocument(createDocument("field", Long.MIN_VALUE));
    writer.addDocument(createDocument("field", Long.MAX_VALUE));

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    assertEquals(2, searcher.count(createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(1, searcher.count(createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE - 1)));
    assertEquals(1, searcher.count(createQuery("field", Long.MIN_VALUE + 1, Long.MAX_VALUE)));
    assertEquals(0, searcher.count(createQuery("field", Long.MIN_VALUE + 1, Long.MAX_VALUE - 1)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testMissingValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
    sortField.setMissingValue(random().nextLong());
    iwc.setIndexSort(new Sort(sortField));
    IndexWriter writer = new IndexWriter(dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 35));

    writer.addDocument(createDocument("other-field", 0));
    writer.addDocument(createDocument("other-field", 10));
    writer.addDocument(createDocument("other-field", 20));

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    assertEquals(2, searcher.count(createQuery("field", -70, 0)));
    assertEquals(2, searcher.count(createQuery("field", -2, 35)));

    assertEquals(4, searcher.count(createQuery("field", -80, 35)));
    assertEquals(4, searcher.count(createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testNoIndexSort() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, iwc);
    writer.addDocument(createDocument("field", 0));

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    expectThrows(IllegalArgumentException.class,
        () -> searcher.count(createQuery("field", -80, 80)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortOnOtherField() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("other-field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter writer = new IndexWriter(dir, iwc);

    writer.addDocument(createDocument("field", 0));

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    expectThrows(IllegalArgumentException.class,
        () -> searcher.count(createQuery("field", -80, 80)));

    writer.close();
    reader.close();
    dir.close();
  }


  public void testMultiDocValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    IndexWriter writer = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("field", 0));
    doc.add(new SortedNumericDocValuesField("field", 10));
    writer.addDocument(doc);

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);

    expectThrows(RuntimeException.class,
        () -> searcher.count(createQuery("field", -80, 80)));

    writer.close();
    reader.close();
    dir.close();
  }

  private Document createDocument(String field, long value) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(field, value));
    return doc;
  }

  private IndexSortDocValuesRangeQuery createQuery(String field, long lowerValue, long upperValue) {
    return new IndexSortDocValuesRangeQuery(field, lowerValue, upperValue);
  }
}
