package org.apache.lucene.index;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestPayloads extends LuceneTestCase {
    
    // Simple tests to test the payloads
    public void testPayload() throws Exception {
        BytesRef payload = new BytesRef("This is a test!");
        assertEquals("Wrong payload length.", "This is a test!".length(), payload.length);
        
        BytesRef clone = payload.clone();
        assertEquals(payload.length, clone.length);
        for (int i = 0; i < payload.length; i++) {
          assertEquals(payload.bytes[i + payload.offset], clone.bytes[i + clone.offset]);
        }
        
    }

    // Tests whether the DocumentWriter and SegmentMerger correctly enable the
    // payload bit in the FieldInfo
    public void testPayloadFieldBit() throws Exception {
        Directory ram = newDirectory();
        PayloadAnalyzer analyzer = new PayloadAnalyzer();
        IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig( TEST_VERSION_CURRENT, analyzer));
        Document d = new Document();
        // this field won't have any payloads
        d.add(newTextField("f1", "This field has no payloads", Field.Store.NO));
        // this field will have payloads in all docs, however not for all term positions,
        // so this field is used to check if the DocumentWriter correctly enables the payloads bit
        // even if only some term positions have payloads
        d.add(newTextField("f2", "This field has payloads in all docs", Field.Store.NO));
        d.add(newTextField("f2", "This field has payloads in all docs NO PAYLOAD", Field.Store.NO));
        // this field is used to verify if the SegmentMerger enables payloads for a field if it has payloads 
        // enabled in only some documents
        d.add(newTextField("f3", "This field has payloads in some docs", Field.Store.NO));
        // only add payload data for field f2
        analyzer.setPayloadData("f2", "somedata".getBytes("UTF-8"), 0, 1);
        writer.addDocument(d);
        // flush
        writer.close();

      SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(ram));
        FieldInfos fi = reader.getFieldInfos();
        assertFalse("Payload field bit should not be set.", fi.fieldInfo("f1").hasPayloads());
        assertTrue("Payload field bit should be set.", fi.fieldInfo("f2").hasPayloads());
        assertFalse("Payload field bit should not be set.", fi.fieldInfo("f3").hasPayloads());
        reader.close();
        
        // now we add another document which has payloads for field f3 and verify if the SegmentMerger
        // enabled payloads for that field
        analyzer = new PayloadAnalyzer(); // Clear payload state for each field
        writer = new IndexWriter(ram, newIndexWriterConfig( TEST_VERSION_CURRENT,
            analyzer).setOpenMode(OpenMode.CREATE));
        d = new Document();
        d.add(newTextField("f1", "This field has no payloads", Field.Store.NO));
        d.add(newTextField("f2", "This field has payloads in all docs", Field.Store.NO));
        d.add(newTextField("f2", "This field has payloads in all docs", Field.Store.NO));
        d.add(newTextField("f3", "This field has payloads in some docs", Field.Store.NO));
        // add payload data for field f2 and f3
        analyzer.setPayloadData("f2", "somedata".getBytes("UTF-8"), 0, 1);
        analyzer.setPayloadData("f3", "somedata".getBytes("UTF-8"), 0, 3);
        writer.addDocument(d);

        // force merge
        writer.forceMerge(1);
        // flush
        writer.close();

      reader = getOnlySegmentReader(DirectoryReader.open(ram));
        fi = reader.getFieldInfos();
        assertFalse("Payload field bit should not be set.", fi.fieldInfo("f1").hasPayloads());
        assertTrue("Payload field bit should be set.", fi.fieldInfo("f2").hasPayloads());
        assertTrue("Payload field bit should be set.", fi.fieldInfo("f3").hasPayloads());
        reader.close();
        ram.close();
    }

    // Tests if payloads are correctly stored and loaded using both RamDirectory and FSDirectory
    public void testPayloadsEncoding() throws Exception {
        Directory dir = newDirectory();
        performTest(dir);
        dir.close();
    }
    
    // builds an index with payloads in the given Directory and performs
    // different tests to verify the payload encoding
    private void performTest(Directory dir) throws Exception {
        PayloadAnalyzer analyzer = new PayloadAnalyzer();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, analyzer)
            .setOpenMode(OpenMode.CREATE)
            .setMergePolicy(newLogMergePolicy()));
        
        // should be in sync with value in TermInfosWriter
        final int skipInterval = 16;
        
        final int numTerms = 5;
        final String fieldName = "f1";
        
        int numDocs = skipInterval + 1; 
        // create content for the test documents with just a few terms
        Term[] terms = generateTerms(fieldName, numTerms);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < terms.length; i++) {
            sb.append(terms[i].text());
            sb.append(" ");
        }
        String content = sb.toString();
        
        
        int payloadDataLength = numTerms * numDocs * 2 + numTerms * numDocs * (numDocs - 1) / 2;
        byte[] payloadData = generateRandomData(payloadDataLength);
        
        Document d = new Document();
        d.add(newTextField(fieldName, content, Field.Store.NO));
        // add the same document multiple times to have the same payload lengths for all
        // occurrences within two consecutive skip intervals
        int offset = 0;
        for (int i = 0; i < 2 * numDocs; i++) {
            analyzer = new PayloadAnalyzer(fieldName, payloadData, offset, 1);
            offset += numTerms;
            writer.addDocument(d, analyzer);
        }
        
        // make sure we create more than one segment to test merging
        writer.commit();
        
        // now we make sure to have different payload lengths next at the next skip point        
        for (int i = 0; i < numDocs; i++) {
            analyzer = new PayloadAnalyzer(fieldName, payloadData, offset, i);
            offset += i * numTerms;
            writer.addDocument(d, analyzer);
        }
        
        writer.forceMerge(1);
        // flush
        writer.close();
        
        
        /*
         * Verify the index
         * first we test if all payloads are stored correctly
         */        
        IndexReader reader = DirectoryReader.open(dir);

        byte[] verifyPayloadData = new byte[payloadDataLength];
        offset = 0;
        DocsAndPositionsEnum[] tps = new DocsAndPositionsEnum[numTerms];
        for (int i = 0; i < numTerms; i++) {
          tps[i] = MultiFields.getTermPositionsEnum(reader,
                                                    MultiFields.getLiveDocs(reader),
                                                    terms[i].field(),
                                                    new BytesRef(terms[i].text()));
        }
        
        while (tps[0].nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            for (int i = 1; i < numTerms; i++) {
                tps[i].nextDoc();
            }
            int freq = tps[0].freq();

            for (int i = 0; i < freq; i++) {
                for (int j = 0; j < numTerms; j++) {
                    tps[j].nextPosition();
                    BytesRef br = tps[j].getPayload();
                    if (br != null) {
                      System.arraycopy(br.bytes, br.offset, verifyPayloadData, offset, br.length);
                      offset += br.length;
                    }
                }
            }
        }
        
        assertByteArrayEquals(payloadData, verifyPayloadData);
        
        /*
         *  test lazy skipping
         */        
        DocsAndPositionsEnum tp = MultiFields.getTermPositionsEnum(reader,
                                                                   MultiFields.getLiveDocs(reader),
                                                                   terms[0].field(),
                                                                   new BytesRef(terms[0].text()));
        tp.nextDoc();
        tp.nextPosition();
        // NOTE: prior rev of this test was failing to first
        // call next here:
        tp.nextDoc();
        // now we don't read this payload
        tp.nextPosition();
        BytesRef payload = tp.getPayload();
        assertEquals("Wrong payload length.", 1, payload.length);
        assertEquals(payload.bytes[payload.offset], payloadData[numTerms]);
        tp.nextDoc();
        tp.nextPosition();
        
        // we don't read this payload and skip to a different document
        tp.advance(5);
        tp.nextPosition();
        payload = tp.getPayload();
        assertEquals("Wrong payload length.", 1, payload.length);
        assertEquals(payload.bytes[payload.offset], payloadData[5 * numTerms]);
                
        
        /*
         * Test different lengths at skip points
         */
        tp = MultiFields.getTermPositionsEnum(reader,
                                              MultiFields.getLiveDocs(reader),
                                              terms[1].field(),
                                              new BytesRef(terms[1].text()));
        tp.nextDoc();
        tp.nextPosition();
        assertEquals("Wrong payload length.", 1, tp.getPayload().length);
        tp.advance(skipInterval - 1);
        tp.nextPosition();
        assertEquals("Wrong payload length.", 1, tp.getPayload().length);
        tp.advance(2 * skipInterval - 1);
        tp.nextPosition();
        assertEquals("Wrong payload length.", 1, tp.getPayload().length);
        tp.advance(3 * skipInterval - 1);
        tp.nextPosition();
        assertEquals("Wrong payload length.", 3 * skipInterval - 2 * numDocs - 1, tp.getPayload().length);
        
        reader.close();
        
        // test long payload
        analyzer = new PayloadAnalyzer();
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT,
            analyzer).setOpenMode(OpenMode.CREATE));
        String singleTerm = "lucene";
        
        d = new Document();
        d.add(newTextField(fieldName, singleTerm, Field.Store.NO));
        // add a payload whose length is greater than the buffer size of BufferedIndexOutput
        payloadData = generateRandomData(2000);
        analyzer.setPayloadData(fieldName, payloadData, 100, 1500);
        writer.addDocument(d);

        
        writer.forceMerge(1);
        // flush
        writer.close();
        
        reader = DirectoryReader.open(dir);
        tp = MultiFields.getTermPositionsEnum(reader,
                                              MultiFields.getLiveDocs(reader),
                                              fieldName,
                                              new BytesRef(singleTerm));
        tp.nextDoc();
        tp.nextPosition();
        
        BytesRef br = tp.getPayload();
        verifyPayloadData = new byte[br.length];
        byte[] portion = new byte[1500];
        System.arraycopy(payloadData, 100, portion, 0, 1500);
        
        assertByteArrayEquals(portion, br.bytes, br.offset, br.length);
        reader.close();
        
    }
    
    static final Charset utf8 = Charset.forName("UTF-8");
    private void generateRandomData(byte[] data) {
      // this test needs the random data to be valid unicode
      String s = _TestUtil.randomFixedByteLengthUnicodeString(random(), data.length);
      byte b[] = s.getBytes(utf8);
      assert b.length == data.length;
      System.arraycopy(b, 0, data, 0, b.length);
    }

    private byte[] generateRandomData(int n) {
        byte[] data = new byte[n];
        generateRandomData(data);
        return data;
    }
    
    private Term[] generateTerms(String fieldName, int n) {
        int maxDigits = (int) (Math.log(n) / Math.log(10));
        Term[] terms = new Term[n];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.setLength(0);
            sb.append("t");
            int zeros = maxDigits - (int) (Math.log(i) / Math.log(10));
            for (int j = 0; j < zeros; j++) {
                sb.append("0");
            }
            sb.append(i);
            terms[i] = new Term(fieldName, sb.toString());
        }
        return terms;
    }


    void assertByteArrayEquals(byte[] b1, byte[] b2) {
        if (b1.length != b2.length) {
          fail("Byte arrays have different lengths: " + b1.length + ", " + b2.length);
        }
        
        for (int i = 0; i < b1.length; i++) {
          if (b1[i] != b2[i]) {
            fail("Byte arrays different at index " + i + ": " + b1[i] + ", " + b2[i]);
          }
        }
      }    
    
  void assertByteArrayEquals(byte[] b1, byte[] b2, int b2offset, int b2length) {
        if (b1.length != b2length) {
          fail("Byte arrays have different lengths: " + b1.length + ", " + b2length);
        }
        
        for (int i = 0; i < b1.length; i++) {
          if (b1[i] != b2[b2offset+i]) {
            fail("Byte arrays different at index " + i + ": " + b1[i] + ", " + b2[b2offset+i]);
          }
        }
      }    
    
    
    /**
     * This Analyzer uses an WhitespaceTokenizer and PayloadFilter.
     */
    private static class PayloadAnalyzer extends Analyzer {
        Map<String,PayloadData> fieldToData = new HashMap<String,PayloadData>();

        public PayloadAnalyzer() {
          super(new PerFieldReuseStrategy());
        }
        
        public PayloadAnalyzer(String field, byte[] data, int offset, int length) {
            super(new PerFieldReuseStrategy());
            setPayloadData(field, data, offset, length);
        }

        void setPayloadData(String field, byte[] data, int offset, int length) {
            fieldToData.put(field, new PayloadData(data, offset, length));
        }
        
        @Override
        public TokenStreamComponents createComponents(String fieldName, Reader reader) {
            PayloadData payload =  fieldToData.get(fieldName);
            Tokenizer ts = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
            TokenStream tokenStream = (payload != null) ?
                new PayloadFilter(ts, payload.data, payload.offset, payload.length) : ts;
            return new TokenStreamComponents(ts, tokenStream);
        }
        
        private static class PayloadData {
            byte[] data;
            int offset;
            int length;

            PayloadData(byte[] data, int offset, int length) {
                this.data = data;
                this.offset = offset;
                this.length = length;
            }
        }
    }

    
    /**
     * This Filter adds payloads to the tokens.
     */
    private static class PayloadFilter extends TokenFilter {
        private byte[] data;
        private int length;
        private int offset;
        private int startOffset;
        PayloadAttribute payloadAtt;
        CharTermAttribute termAttribute;
        
        public PayloadFilter(TokenStream in, byte[] data, int offset, int length) {
            super(in);
            this.data = data;
            this.length = length;
            this.offset = offset;
            this.startOffset = offset;
            payloadAtt = addAttribute(PayloadAttribute.class);
            termAttribute = addAttribute(CharTermAttribute.class);
        }
        
        @Override
        public boolean incrementToken() throws IOException {
            boolean hasNext = input.incrementToken();
            if (!hasNext) {
              return false;
            }

            // Some values of the same field are to have payloads and others not
            if (offset + length <= data.length && !termAttribute.toString().endsWith("NO PAYLOAD")) {
              BytesRef p = new BytesRef(data, offset, length);
              payloadAtt.setPayload(p);
              offset += length;
            } else {
              payloadAtt.setPayload(null);
            }

            return true;
        }

      @Override
      public void reset() throws IOException {
        super.reset();
        this.offset = startOffset;
      }
    }
    
    public void testThreadSafety() throws Exception {
        final int numThreads = 5;
        final int numDocs = atLeast(50);
        final ByteArrayPool pool = new ByteArrayPool(numThreads, 5);
        
        Directory dir = newDirectory();
        final IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( 
            TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        final String field = "test";
        
        Thread[] ingesters = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            ingesters[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < numDocs; j++) {
                            Document d = new Document();
                            d.add(new TextField(field, new PoolingPayloadTokenStream(pool)));
                            writer.addDocument(d);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail(e.toString());
                    }
                }
            };
            ingesters[i].start();
        }
        
        for (int i = 0; i < numThreads; i++) {
          ingesters[i].join();
        }
        writer.close();
        IndexReader reader = DirectoryReader.open(dir);
        TermsEnum terms = MultiFields.getFields(reader).terms(field).iterator(null);
        Bits liveDocs = MultiFields.getLiveDocs(reader);
        DocsAndPositionsEnum tp = null;
        while (terms.next() != null) {
          String termText = terms.term().utf8ToString();
          tp = terms.docsAndPositions(liveDocs, tp);
          while(tp.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int freq = tp.freq();
            for (int i = 0; i < freq; i++) {
              tp.nextPosition();
              final BytesRef payload = tp.getPayload();
              assertEquals(termText, payload.utf8ToString());
            }
          }
        }
        reader.close();
        dir.close();
        assertEquals(pool.size(), numThreads);
    }
    
    private class PoolingPayloadTokenStream extends TokenStream {
        private byte[] payload;
        private boolean first;
        private ByteArrayPool pool;
        private String term;

        CharTermAttribute termAtt;
        PayloadAttribute payloadAtt;
        
        PoolingPayloadTokenStream(ByteArrayPool pool) {
            this.pool = pool;
            payload = pool.get();
            generateRandomData(payload);
            term = new String(payload, 0, payload.length, utf8);
            first = true;
            payloadAtt = addAttribute(PayloadAttribute.class);
            termAtt = addAttribute(CharTermAttribute.class);
        }
        
        @Override
        public boolean incrementToken() throws IOException {
            if (!first) return false;
            first = false;
            clearAttributes();
            termAtt.append(term);
            payloadAtt.setPayload(new BytesRef(payload));
            return true;
        }
        
        @Override
        public void close() throws IOException {
            pool.release(payload);
        }
        
    }
    
    private static class ByteArrayPool {
        private List<byte[]> pool;
        
        ByteArrayPool(int capacity, int size) {
            pool = new ArrayList<byte[]>();
            for (int i = 0; i < capacity; i++) {
                pool.add(new byte[size]);
            }
        }
    
        synchronized byte[] get() {
            return pool.remove(0);
        }
        
        synchronized void release(byte[] b) {
            pool.add(b);
        }
        
        synchronized int size() {
            return pool.size();
        }
    }

  public void testAcrossFields() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
                                                     new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true));
    Document doc = new Document();
    doc.add(new TextField("hasMaybepayload", "here we go", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    writer = new RandomIndexWriter(random(), dir,
                                   new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true));
    doc = new Document();
    doc.add(new TextField("hasMaybepayload2", "here we go", Field.Store.YES));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.forceMerge(1);
    writer.close();

    dir.close();
  }
  
  /** some docs have payload att, some not */
  public void testMixupDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    Field field = new TextField("field", "", Field.Store.NO);
    TokenStream ts = new MockTokenizer(new StringReader("here we go"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    doc.add(field);
    writer.addDocument(doc);
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    writer.addDocument(doc);
    ts = new MockTokenizer(new StringReader("another"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    AtomicReader sr = SlowCompositeReaderWrapper.wrap(reader);
    DocsAndPositionsEnum de = sr.termPositionsEnum(new Term("field", "withPayload"));
    de.nextDoc();
    de.nextPosition();
    assertEquals(new BytesRef("test"), de.getPayload());
    writer.close();
    reader.close();
    dir.close();
  }
  
  /** some field instances have payload att, some not */
  public void testMixupMultiValued() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field field = new TextField("field", "", Field.Store.NO);
    TokenStream ts = new MockTokenizer(new StringReader("here we go"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field.setTokenStream(ts);
    doc.add(field);
    Field field2 = new TextField("field", "", Field.Store.NO);
    Token withPayload = new Token("withPayload", 0, 11);
    withPayload.setPayload(new BytesRef("test"));
    ts = new CannedTokenStream(withPayload);
    assertTrue(ts.hasAttribute(PayloadAttribute.class));
    field2.setTokenStream(ts);
    doc.add(field2);
    Field field3 = new TextField("field", "", Field.Store.NO);
    ts = new MockTokenizer(new StringReader("nopayload"), MockTokenizer.WHITESPACE, true);
    assertFalse(ts.hasAttribute(PayloadAttribute.class));
    field3.setTokenStream(ts);
    doc.add(field3);
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    SegmentReader sr = getOnlySegmentReader(reader);
    DocsAndPositionsEnum de = sr.termPositionsEnum(new Term("field", "withPayload"));
    de.nextDoc();
    de.nextPosition();
    assertEquals(new BytesRef("test"), de.getPayload());
    writer.close();
    reader.close();
    dir.close();
  }
  
}
