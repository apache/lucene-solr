package org.apache.lucene.index;

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
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;

public class TestPayloads extends LuceneTestCase {
    
    // Simple tests to test the Payload class
    public void testPayload() throws Exception {
        byte[] testData = "This is a test!".getBytes();
        Payload payload = new Payload(testData);
        assertEquals("Wrong payload length.", testData.length, payload.length());
        
        // test copyTo()
        byte[] target = new byte[testData.length - 1];
        try {
            payload.copyTo(target, 0);
            fail("Expected exception not thrown");
        } catch (Exception expected) {
            // expected exception
        }
        
        target = new byte[testData.length + 3];
        payload.copyTo(target, 3);
        
        for (int i = 0; i < testData.length; i++) {
            assertEquals(testData[i], target[i + 3]);
        }
        

        // test toByteArray()
        target = payload.toByteArray();
        assertByteArrayEquals(testData, target);

        // test byteAt()
        for (int i = 0; i < testData.length; i++) {
            assertEquals(payload.byteAt(i), testData[i]);
        }
        
        try {
            payload.byteAt(testData.length + 1);
            fail("Expected exception not thrown");
        } catch (Exception expected) {
            // expected exception
        }
        
        Payload clone = (Payload) payload.clone();
        assertEquals(payload.length(), clone.length());
        for (int i = 0; i < payload.length(); i++) {
          assertEquals(payload.byteAt(i), clone.byteAt(i));
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
        d.add(newField("f1", "This field has no payloads", Field.Store.NO, Field.Index.ANALYZED));
        // this field will have payloads in all docs, however not for all term positions,
        // so this field is used to check if the DocumentWriter correctly enables the payloads bit
        // even if only some term positions have payloads
        d.add(newField("f2", "This field has payloads in all docs", Field.Store.NO, Field.Index.ANALYZED));
        d.add(newField("f2", "This field has payloads in all docs", Field.Store.NO, Field.Index.ANALYZED));
        // this field is used to verify if the SegmentMerger enables payloads for a field if it has payloads 
        // enabled in only some documents
        d.add(newField("f3", "This field has payloads in some docs", Field.Store.NO, Field.Index.ANALYZED));
        // only add payload data for field f2
        analyzer.setPayloadData("f2", 1, "somedata".getBytes(), 0, 1);
        writer.addDocument(d);
        // flush
        writer.close();

      SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram, false));
        FieldInfos fi = reader.fieldInfos();
        assertFalse("Payload field bit should not be set.", fi.fieldInfo("f1").storePayloads);
        assertTrue("Payload field bit should be set.", fi.fieldInfo("f2").storePayloads);
        assertFalse("Payload field bit should not be set.", fi.fieldInfo("f3").storePayloads);
        reader.close();
        
        // now we add another document which has payloads for field f3 and verify if the SegmentMerger
        // enabled payloads for that field
        writer = new IndexWriter(ram, newIndexWriterConfig( TEST_VERSION_CURRENT,
            analyzer).setOpenMode(OpenMode.CREATE));
        d = new Document();
        d.add(newField("f1", "This field has no payloads", Field.Store.NO, Field.Index.ANALYZED));
        d.add(newField("f2", "This field has payloads in all docs", Field.Store.NO, Field.Index.ANALYZED));
        d.add(newField("f2", "This field has payloads in all docs", Field.Store.NO, Field.Index.ANALYZED));
        d.add(newField("f3", "This field has payloads in some docs", Field.Store.NO, Field.Index.ANALYZED));
        // add payload data for field f2 and f3
        analyzer.setPayloadData("f2", "somedata".getBytes(), 0, 1);
        analyzer.setPayloadData("f3", "somedata".getBytes(), 0, 3);
        writer.addDocument(d);

        // force merge
        writer.optimize();
        // flush
        writer.close();

      reader = getOnlySegmentReader(IndexReader.open(ram, false));
        fi = reader.fieldInfos();
        assertFalse("Payload field bit should not be set.", fi.fieldInfo("f1").storePayloads);
        assertTrue("Payload field bit should be set.", fi.fieldInfo("f2").storePayloads);
        assertTrue("Payload field bit should be set.", fi.fieldInfo("f3").storePayloads);
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
        d.add(newField(fieldName, content, Field.Store.NO, Field.Index.ANALYZED));
        // add the same document multiple times to have the same payload lengths for all
        // occurrences within two consecutive skip intervals
        int offset = 0;
        for (int i = 0; i < 2 * numDocs; i++) {
            analyzer.setPayloadData(fieldName, payloadData, offset, 1);
            offset += numTerms;
            writer.addDocument(d);
        }
        
        // make sure we create more than one segment to test merging
        writer.commit();
        
        // now we make sure to have different payload lengths next at the next skip point        
        for (int i = 0; i < numDocs; i++) {
            analyzer.setPayloadData(fieldName, payloadData, offset, i);
            offset += i * numTerms;
            writer.addDocument(d);
        }
        
        writer.optimize();
        // flush
        writer.close();
        
        
        /*
         * Verify the index
         * first we test if all payloads are stored correctly
         */        
        IndexReader reader = IndexReader.open(dir, true);

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
                    if (tps[j].hasPayload()) {
                      BytesRef br = tps[j].getPayload();
                      System.arraycopy(br.bytes, br.offset, verifyPayloadData, offset, br.length);
                      offset += br.length;
                      // Just to ensure all codecs can
                      // handle a caller that mucks with the
                      // returned payload:
                      if (rarely()) {
                        br.bytes = new byte[random.nextInt(5)];
                      }
                      br.length = 0;
                      br.offset = 0;
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
        
        /*
         * Test multiple call of getPayload()
         */
        assertFalse(tp.hasPayload());
        
        reader.close();
        
        // test long payload
        analyzer = new PayloadAnalyzer();
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT,
            analyzer).setOpenMode(OpenMode.CREATE));
        String singleTerm = "lucene";
        
        d = new Document();
        d.add(newField(fieldName, singleTerm, Field.Store.NO, Field.Index.ANALYZED));
        // add a payload whose length is greater than the buffer size of BufferedIndexOutput
        payloadData = generateRandomData(2000);
        analyzer.setPayloadData(fieldName, payloadData, 100, 1500);
        writer.addDocument(d);

        
        writer.optimize();
        // flush
        writer.close();
        
        reader = IndexReader.open(dir, true);
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
    
    private void generateRandomData(byte[] data) {
        random.nextBytes(data);
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
        
        void setPayloadData(String field, byte[] data, int offset, int length) {
            fieldToData.put(field, new PayloadData(0, data, offset, length));
        }

        void setPayloadData(String field, int numFieldInstancesToSkip, byte[] data, int offset, int length) {
            fieldToData.put(field, new PayloadData(numFieldInstancesToSkip, data, offset, length));
        }
        
        @Override
        public TokenStream tokenStream(String fieldName, Reader reader) {
            PayloadData payload =  fieldToData.get(fieldName);
            TokenStream ts = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
            if (payload != null) {
                if (payload.numFieldInstancesToSkip == 0) {
                    ts = new PayloadFilter(ts, payload.data, payload.offset, payload.length);
                } else {
                    payload.numFieldInstancesToSkip--;
                }
            }
            return ts;
        }
        
        private static class PayloadData {
            byte[] data;
            int offset;
            int length;
            int numFieldInstancesToSkip;
            
            PayloadData(int skip, byte[] data, int offset, int length) {
                numFieldInstancesToSkip = skip;
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
        Payload payload = new Payload();
        PayloadAttribute payloadAtt;
        
        public PayloadFilter(TokenStream in, byte[] data, int offset, int length) {
            super(in);
            this.data = data;
            this.length = length;
            this.offset = offset;
            payloadAtt = addAttribute(PayloadAttribute.class);
        }
        
        @Override
        public boolean incrementToken() throws IOException {
            boolean hasNext = input.incrementToken();
            if (hasNext) {
                if (offset + length <= data.length) {
                    Payload p = new Payload();
                    payloadAtt.setPayload(p);
                    p.setData(data, offset, length);
                    offset += length;                
                } else {
                    payloadAtt.setPayload(null);
                }
            }
            
            return hasNext;
        }
    }
    
    public void testThreadSafety() throws Exception {
        final int numThreads = 5;
        final int numDocs = atLeast(50);
        final ByteArrayPool pool = new ByteArrayPool(numThreads, 5);
        
        Directory dir = newDirectory();
        final IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( 
            TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        final String field = "test";
        
        Thread[] ingesters = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            ingesters[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < numDocs; j++) {
                            Document d = new Document();
                            d.add(new Field(field, new PoolingPayloadTokenStream(pool)));
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
        IndexReader reader = IndexReader.open(dir, true);
        TermsEnum terms = MultiFields.getFields(reader).terms(field).iterator();
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
              assertEquals(termText, pool.bytesToString(payload.bytes, payload.offset, payload.length));
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
            term = pool.bytesToString(payload, 0, payload.length);
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
            payloadAtt.setPayload(new Payload(payload));
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
        
        static String bytesToString(byte[] bytes, int start, int length) {
            String s = new String(bytes, start, length);
            BytesRef utf8Result = new BytesRef(10);
            UnicodeUtil.UTF16toUTF8(s, 0, s.length(), utf8Result);
            try {
                return new String(utf8Result.bytes, 0, utf8Result.length, "UTF-8");
            } catch (UnsupportedEncodingException uee) {
                return null;
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
    RandomIndexWriter writer = new RandomIndexWriter(random, dir,
                                                     new MockAnalyzer(random, MockTokenizer.WHITESPACE, true));
    Document doc = new Document();
    doc.add(new Field("hasMaybepayload", "here we go", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    writer = new RandomIndexWriter(random, dir,
                                   new MockAnalyzer(random, MockTokenizer.WHITESPACE, true));
    doc = new Document();
    doc.add(new Field("hasMaybepayload2", "here we go", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.optimize();
    writer.close();

    dir.close();
  }
}
