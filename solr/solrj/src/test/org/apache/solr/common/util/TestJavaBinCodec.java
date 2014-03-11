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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class TestJavaBinCodec extends LuceneTestCase {

 private static final String SOLRJ_JAVABIN_BACKCOMPAT_BIN = "/solrj/javabin_backcompat.bin";
private final String BIN_FILE_LOCATION = "./solr/solrj/src/test-files/solrj/javabin_backcompat.bin";

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
  public void testBackCompat() {
    List iteratorAsList = null;
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

        } else {
          assertEquals(unmarshaledObj.get(i), matchObj.get(i));
        }

      }
    } catch (IOException e) {
      fail(e.getMessage());
    }

  }

  @Test
  public void testForwardCompat() {
    JavaBinCodec javabin = new JavaBinCodec();
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    Object data = generateAllDataTypes();
    try {
      javabin.marshal(data, os);
      byte[] newFormatBytes = os.toByteArray();

      InputStream is = getClass().getResourceAsStream(SOLRJ_JAVABIN_BACKCOMPAT_BIN);
      byte[] currentFormatBytes = IOUtils.toByteArray(is);

      for (int i = 1; i < currentFormatBytes.length; i++) {//ignore the first byte. It is version information
        assertEquals(currentFormatBytes[i], newFormatBytes[i]);
      }

    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }

  public void genBinaryFile() throws IOException {
    JavaBinCodec javabin = new JavaBinCodec();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    
    Object data = generateAllDataTypes();
    
    javabin.marshal(data, os);
    byte[] out = os.toByteArray();
    FileOutputStream fs = new FileOutputStream(new File(BIN_FILE_LOCATION));
    BufferedOutputStream bos = new BufferedOutputStream(fs);
    bos.write(out);
    bos.close();
  }

  public static void main(String[] args) throws IOException {
    TestJavaBinCodec test = new TestJavaBinCodec();
    test.genBinaryFile();
  }

}
