package org.apache.solr.common.util;

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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.util.ConcurrentLRUCache;
import org.junit.Test;
import org.noggit.CharArr;

public class TestJavaBinCodec extends SolrTestCaseJ4 {

  private static final String SOLRJ_JAVABIN_BACKCOMPAT_BIN = "/solrj/javabin_backcompat.bin";
  private final String BIN_FILE_LOCATION = "./solr/solrj/src/test-files/solrj/javabin_backcompat.bin";

  private static final String SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS = "/solrj/javabin_backcompat_child_docs.bin";
  private final String BIN_FILE_LOCATION_CHILD_DOCS = "./solr/solrj/src/test-files/solrj/javabin_backcompat_child_docs.bin";

  public void testStrings() throws Exception {
    JavaBinCodec javabin = new JavaBinCodec();
    for (int i = 0; i < 10000 * RANDOM_MULTIPLIER; i++) {
      String s = TestUtil.randomUnicodeString(random());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      javabin.marshal(s, os);
      ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
      Object o = javabin.unmarshal(is);
      assertEquals(s, o);
    }
  }

  private SolrDocument generateSolrDocumentWithChildDocs() {
    SolrDocument parentDocument = new SolrDocument();
    parentDocument.addField("id", "1");
    parentDocument.addField("subject", "parentDocument");

    SolrDocument childDocument = new SolrDocument();
    childDocument.addField("id", "2");
    childDocument.addField("cat", "foo");

    SolrDocument secondKid = new SolrDocument();
    secondKid.addField("id", "22");
    secondKid.addField("cat", "bar");

    SolrDocument grandChildDocument = new SolrDocument();
    grandChildDocument.addField("id", "3");

    childDocument.addChildDocument(grandChildDocument);
    parentDocument.addChildDocument(childDocument);
    parentDocument.addChildDocument(secondKid);

    return parentDocument;
  }

  private List<Object> generateAllDataTypes() {
    List<Object> types = new ArrayList<>();

    types.add(null); //NULL
    types.add(true);
    types.add(false);
    types.add((byte) 1);
    types.add((short) 2);
    types.add((double) 3);

    types.add(-4);
    types.add(4);
    types.add(42);

    types.add((long) -5);
    types.add((long) 5);
    types.add((long) 50);

    types.add((float) 6);
    types.add(new Date(0));

    Map<Integer, Integer> map = new HashMap<>();
    map.put(1, 2);
    types.add(map);

    SolrDocument doc = new SolrDocument();
    doc.addField("foo", "bar");
    types.add(doc);

    SolrDocumentList solrDocs = new SolrDocumentList();
    solrDocs.setMaxScore(1.0f);
    solrDocs.setNumFound(1);
    solrDocs.setStart(0);
    solrDocs.add(0, doc);
    types.add(solrDocs);

    types.add(new byte[] {1,2,3,4,5});

    // TODO?
    // List<String> list = new ArrayList<String>();
    // list.add("one");
    // types.add(list.iterator());

    types.add((byte) 15); //END

    SolrInputDocument idoc = new SolrInputDocument();
    idoc.addField("foo", "bar");
    types.add(idoc);

    SolrInputDocument parentDoc = new SolrInputDocument();
    parentDoc.addField("foo", "bar");
    SolrInputDocument childDoc = new SolrInputDocument();
    childDoc.addField("foo", "bar");
    parentDoc.addChildDocument(childDoc);
    types.add(parentDoc);

    types.add(new EnumFieldValue(1, "foo"));

    types.add(map.entrySet().iterator().next()); //Map.Entry

    types.add((byte) (1 << 5)); //TAG_AND_LEN

    types.add("foo");
    types.add(1);
    types.add((long) 2);

    SimpleOrderedMap simpleOrderedMap = new SimpleOrderedMap();
    simpleOrderedMap.add("bar", "barbar");
    types.add(simpleOrderedMap);

    NamedList<String> nl = new NamedList<>();
    nl.add("foo", "barbar");
    types.add(nl);

    return types;
  }

  @Test
  public void testBackCompat() throws IOException {
    JavaBinCodec javabin = new JavaBinCodec(){
      @Override
      public List<Object> readIterator(DataInputInputStream fis) throws IOException {
        return super.readIterator(fis);
      }
    };
    try {
      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN);
      List<Object> unmarshaledObj = (List<Object>) javabin.unmarshal(is);
      List<Object> matchObj = generateAllDataTypes();

      assertEquals(unmarshaledObj.size(), matchObj.size());
      for(int i=0; i < unmarshaledObj.size(); i++) {

        if(unmarshaledObj.get(i) instanceof byte[] && matchObj.get(i) instanceof byte[]) {
          byte[] b1 = (byte[]) unmarshaledObj.get(i);
          byte[] b2 = (byte[]) matchObj.get(i);
          assertTrue(Arrays.equals(b1, b2));
        } else if(unmarshaledObj.get(i) instanceof SolrDocument && matchObj.get(i) instanceof SolrDocument ) {
          assertTrue(compareSolrDocument(unmarshaledObj.get(i), matchObj.get(i)));
        } else if(unmarshaledObj.get(i) instanceof SolrDocumentList && matchObj.get(i) instanceof SolrDocumentList ) {
          assertTrue(compareSolrDocumentList(unmarshaledObj.get(i), matchObj.get(i)));
        } else if(unmarshaledObj.get(i) instanceof SolrInputDocument && matchObj.get(i) instanceof SolrInputDocument) {
          assertTrue(compareSolrInputDocument(unmarshaledObj.get(i), matchObj.get(i)));
        } else if(unmarshaledObj.get(i) instanceof SolrInputField && matchObj.get(i) instanceof SolrInputField) {
          assertTrue(assertSolrInputFieldEquals(unmarshaledObj.get(i), matchObj.get(i)));
        } else {
          assertEquals(unmarshaledObj.get(i), matchObj.get(i));
        }

      }
    } catch (IOException e) {
      throw e;
    }

  }

  @Test
  public void testBackCompatForSolrDocumentWithChildDocs() throws IOException {
    JavaBinCodec javabin = new JavaBinCodec(){
      @Override
      public List<Object> readIterator(DataInputInputStream fis) throws IOException {
        return super.readIterator(fis);
      }
    };
    try {
      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS);
      SolrDocument sdoc = (SolrDocument) javabin.unmarshal(is);
      SolrDocument matchSolrDoc = generateSolrDocumentWithChildDocs();
      assertTrue(compareSolrDocument(sdoc, matchSolrDoc));
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testForwardCompat() throws IOException {
    JavaBinCodec javabin = new JavaBinCodec();
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    Object data = generateAllDataTypes();
    try {
      javabin.marshal(data, os);
      byte[] newFormatBytes = os.toByteArray();

      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN);
      byte[] currentFormatBytes = IOUtils.toByteArray(is);

      for (int i = 1; i < currentFormatBytes.length; i++) {//ignore the first byte. It is version information
        assertEquals(newFormatBytes[i], currentFormatBytes[i]);
      }

    } catch (IOException e) {
      throw e;
    }

  }

  @Test
  public void testForwardCompatForSolrDocumentWithChildDocs() throws IOException {
    JavaBinCodec javabin = new JavaBinCodec();
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    SolrDocument sdoc = generateSolrDocumentWithChildDocs();
    try {
      javabin.marshal(sdoc, os);
      byte[] newFormatBytes = os.toByteArray();

      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN_CHILD_DOCS);
      byte[] currentFormatBytes = IOUtils.toByteArray(is);

      for (int i = 1; i < currentFormatBytes.length; i++) {//ignore the first byte. It is version information
        assertEquals(newFormatBytes[i], currentFormatBytes[i]);
      }

    } catch (IOException e) {
      throw e;
    }

  }

  @Test
  public void testResponseChildDocuments() throws IOException {


    JavaBinCodec javabin = new JavaBinCodec();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    javabin.marshal(generateSolrDocumentWithChildDocs(), baos);

    SolrDocument result = (SolrDocument) javabin.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(2, result.size());
    assertEquals("1", result.getFieldValue("id"));
    assertEquals("parentDocument", result.getFieldValue("subject"));
    assertTrue(result.hasChildDocuments());

    List<SolrDocument> childDocuments = result.getChildDocuments();
    assertNotNull(childDocuments);
    assertEquals(2, childDocuments.size());
    assertEquals(2, childDocuments.get(0).size());
    assertEquals("2", childDocuments.get(0).getFieldValue("id"));
    assertEquals("foo", childDocuments.get(0).getFieldValue("cat"));

    assertEquals(2, childDocuments.get(1).size());
    assertEquals("22", childDocuments.get(1).getFieldValue("id"));
    assertEquals("bar", childDocuments.get(1).getFieldValue("cat"));
    assertFalse(childDocuments.get(1).hasChildDocuments());
    assertNull(childDocuments.get(1).getChildDocuments());

    assertTrue(childDocuments.get(0).hasChildDocuments());
    List<SolrDocument> grandChildDocuments = childDocuments.get(0).getChildDocuments();
    assertNotNull(grandChildDocuments);
    assertEquals(1, grandChildDocuments.size());
    assertEquals(1, grandChildDocuments.get(0).size());
    assertEquals("3", grandChildDocuments.get(0).getFieldValue("id"));
    assertFalse(grandChildDocuments.get(0).hasChildDocuments());
    assertNull(grandChildDocuments.get(0).getChildDocuments());
  }
  @Test
  public void testStringCaching() throws Exception {
    Map<String, Object> m = ZkNodeProps.makeMap("key1", "val1", "key2", "val2");

    ByteArrayOutputStream os1 = new ByteArrayOutputStream();
    new JavaBinCodec().marshal(m, os1);
    Map m1 = (Map) new JavaBinCodec().unmarshal(new ByteArrayInputStream(os1.toByteArray()));
    ByteArrayOutputStream os2 = new ByteArrayOutputStream();
    new JavaBinCodec().marshal(m, os2);
    Map m2 = (Map) new JavaBinCodec().unmarshal(new ByteArrayInputStream(os2.toByteArray()));
    List l1 = new ArrayList<>(m1.keySet());
    List l2 = new ArrayList<>(m2.keySet());

    assertTrue(l1.get(0).equals(l2.get(0)));
    assertFalse(l1.get(0) == l2.get(0));
    assertTrue(l1.get(1).equals(l2.get(1)));
    assertFalse(l1.get(1) == l2.get(1));

    JavaBinCodec.StringCache stringCache = new JavaBinCodec.StringCache(new Cache<JavaBinCodec.StringBytes, String>() {
      private HashMap<JavaBinCodec.StringBytes, String> cache = new HashMap<>();

      @Override
      public String put(JavaBinCodec.StringBytes key, String val) {
        return cache.put(key, val);
      }

      @Override
      public String get(JavaBinCodec.StringBytes key) {
        return cache.get(key);
      }

      @Override
      public String remove(JavaBinCodec.StringBytes key) {
        return cache.remove(key);
      }

      @Override
      public void clear() {
        cache.clear();

      }
    });


    m1 = (Map) new JavaBinCodec(null, stringCache).unmarshal(new ByteArrayInputStream(os1.toByteArray()));
    m2 = (Map) new JavaBinCodec(null, stringCache).unmarshal(new ByteArrayInputStream(os2.toByteArray()));
    l1 = new ArrayList<>(m1.keySet());
    l2 = new ArrayList<>(m2.keySet());
    assertTrue(l1.get(0).equals(l2.get(0)));
    assertTrue(l1.get(0) == l2.get(0));
    assertTrue(l1.get(1).equals(l2.get(1)));
    assertTrue(l1.get(1) == l2.get(1));


  }

  public void genBinaryFiles() throws IOException {
    JavaBinCodec javabin = new JavaBinCodec();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    
    Object data = generateAllDataTypes();
    
    javabin.marshal(data, os);
    byte[] out = os.toByteArray();
    FileOutputStream fs = new FileOutputStream(new File(BIN_FILE_LOCATION));
    BufferedOutputStream bos = new BufferedOutputStream(fs);
    bos.write(out);
    bos.close();

    //Binary file with child documents
    javabin = new JavaBinCodec();
    SolrDocument sdoc = generateSolrDocumentWithChildDocs();
    os = new ByteArrayOutputStream();
    javabin.marshal(sdoc, os);
    fs = new FileOutputStream(new File(BIN_FILE_LOCATION_CHILD_DOCS));
    bos = new BufferedOutputStream(fs);
    bos.write(os.toByteArray());
    bos.close();

  }

  private void testPerf() throws InterruptedException {
    final ArrayList<JavaBinCodec.StringBytes> l = new ArrayList<>();
    Cache<JavaBinCodec.StringBytes, String> cache = null;
   /* cache = new ConcurrentLRUCache<JavaBinCodec.StringBytes,String>(10000, 9000, 10000, 1000, false, true, null){
      @Override
      public String put(JavaBinCodec.StringBytes key, String val) {
        l.add(key);
        return super.put(key, val);
      }
    };*/
    Runtime.getRuntime().gc();
    printMem("before cache init");

    Cache<JavaBinCodec.StringBytes, String> cache1 = new Cache<JavaBinCodec.StringBytes, String>() {
      private HashMap<JavaBinCodec.StringBytes, String> cache = new HashMap<>();

      @Override
      public String put(JavaBinCodec.StringBytes key, String val) {
        l.add(key);
        return cache.put(key, val);

      }

      @Override
      public String get(JavaBinCodec.StringBytes key) {
        return cache.get(key);
      }

      @Override
      public String remove(JavaBinCodec.StringBytes key) {
        return cache.remove(key);
      }

      @Override
      public void clear() {
        cache.clear();

      }
    };
    JavaBinCodec.StringCache STRING_CACHE = new JavaBinCodec.StringCache(cache1);

//    STRING_CACHE = new JavaBinCodec.StringCache(cache);
    byte[] bytes = new byte[0];
    JavaBinCodec.StringBytes stringBytes = new JavaBinCodec.StringBytes(null,0,0);

    for(int i=0;i<10000;i++) {
      String s = String.valueOf(random().nextLong());
      int end = s.length();
      int maxSize = end * 4;
      if (bytes == null || bytes.length < maxSize) bytes = new byte[maxSize];
      int sz = ByteUtils.UTF16toUTF8(s, 0, end, bytes, 0);
      STRING_CACHE.get(stringBytes.reset(bytes, 0, sz));
    }
    printMem("after cache init");

    long ms = System.currentTimeMillis();
    int ITERS = 1000000;
    int THREADS = 10;

    runInThreads(THREADS,  () -> {
      JavaBinCodec.StringBytes stringBytes1 = new JavaBinCodec.StringBytes(new byte[0], 0,0);
      for(int i=0;i< ITERS;i++){
        JavaBinCodec.StringBytes b = l.get(i % l.size());
        stringBytes1.reset(b.bytes,0,b.bytes.length);
        if(STRING_CACHE.get(stringBytes1) == null) throw new RuntimeException("error");
      }

    });



    printMem("after cache test");
    System.out.println("time taken by LRUCACHE "+ (System.currentTimeMillis()-ms));
    ms = System.currentTimeMillis();

    runInThreads(THREADS,  ()-> {
      String a = null;
      CharArr arr = new CharArr();
      for (int i = 0; i < ITERS; i++) {
        JavaBinCodec.StringBytes sb = l.get(i % l.size());
        arr.reset();
        ByteUtils.UTF8toUTF16(sb.bytes, 0, sb.bytes.length, arr);
        a = arr.toString();
      }
    });

    printMem("after new string test");
    System.out.println("time taken by string creation "+ (System.currentTimeMillis()-ms));



  }

  private void runInThreads(int count,  Runnable runnable) throws InterruptedException {
    ArrayList<Thread> t =new ArrayList();
    for(int i=0;i<count;i++ ) t.add(new Thread(runnable));
    for (Thread thread : t) thread.start();
    for (Thread thread : t) thread.join();
  }

  static void printMem(String head) {
    System.out.println("*************" + head + "***********");
    int mb = 1024*1024;
    //Getting the runtime reference from system
    Runtime runtime = Runtime.getRuntime();
    //Print used memory
    System.out.println("Used Memory:"
        + (runtime.totalMemory() - runtime.freeMemory()) / mb);

    //Print free memory
    System.out.println("Free Memory:"
        + runtime.freeMemory() / mb);


  }

  public static void main(String[] args) throws IOException {
    TestJavaBinCodec test = new TestJavaBinCodec();
    test.genBinaryFiles();
  }

}
