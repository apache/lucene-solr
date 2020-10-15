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
package org.apache.solr.client.solrj.request;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

import static org.apache.solr.common.params.CommonParams.CHILDDOC;

/**
 * Test for UpdateRequestCodec
 *
 * @see org.apache.solr.client.solrj.request.UpdateRequest
 * @since solr 1.4
 */
public class TestUpdateRequestCodec extends SolrTestCase {

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void simple() throws IOException {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.deleteById("*:*");
    updateRequest.deleteById("id:5");
    updateRequest.deleteByQuery("2*");
    updateRequest.deleteByQuery("1*");
    updateRequest.setParam("a", "b");
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1);
    doc.addField("desc", "one");
    doc.addField("desc", "1");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 2);
    doc.addField("desc", "two");
    doc.addField("desc", "2");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 3);
    doc.addField("desc", "three");
    doc.addField("desc", "3");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    Collection<String> foobar = new HashSet<>();
    foobar.add("baz1");
    foobar.add("baz2");
    doc.addField("foobar", foobar);
    updateRequest.add(doc);

//    updateRequest.setWaitFlush(true);
    updateRequest.deleteById("2");
    updateRequest.deleteByQuery("id:3");
    JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    codec.marshal(updateRequest, baos);
    final List<SolrInputDocument> docs = new ArrayList<>();
    JavaBinUpdateRequestCodec.StreamingUpdateHandler handler = (document, req, commitWithin, overwrite) -> {
      Assert.assertNotNull(req.getParams());
      docs.add(document);
    };

    UpdateRequest updateUnmarshalled = codec.unmarshal(new ByteArrayInputStream(baos.toByteArray()), handler);

    for (SolrInputDocument document : docs) {
      updateUnmarshalled.add(document);
    }
    for (int i = 0; i < updateRequest.getDocuments().size(); i++) {
      SolrInputDocument inDoc = updateRequest.getDocuments().get(i);
      SolrInputDocument outDoc = updateUnmarshalled.getDocuments().get(i);
      compareDocs("doc#" + i, inDoc, outDoc);
    }
    Assert.assertEquals(updateUnmarshalled.getDeleteById().get(0),
        updateRequest.getDeleteById().get(0));
    Assert.assertEquals(updateUnmarshalled.getDeleteQuery().get(0),
        updateRequest.getDeleteQuery().get(0));

    assertEquals("b", updateUnmarshalled.getParams().get("a"));
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testIteratable() throws IOException {
    final List<String> values = new ArrayList<>();
    values.add("iterItem1");
    values.add("iterItem2");

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.deleteByQuery("*:*");

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1);
    doc.addField("desc", "one");
    // imagine someone adding a custom Bean that implements Iterable 
    // but is not a Collection
    doc.addField("iter", (Iterable<String>) values::iterator);
    doc.addField("desc", "1");
    updateRequest.add(doc);

    JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    codec.marshal(updateRequest, baos);
    final List<SolrInputDocument> docs = new ArrayList<>();
    JavaBinUpdateRequestCodec.StreamingUpdateHandler handler = (document, req, commitWithin, overwrite) -> {
      Assert.assertNotNull(req.getParams());
      docs.add(document);
    };

    UpdateRequest updateUnmarshalled = codec.unmarshal(new ByteArrayInputStream(baos.toByteArray()), handler);

    for (SolrInputDocument document : docs) {
      updateUnmarshalled.add(document);
    }

    SolrInputDocument outDoc = updateUnmarshalled.getDocuments().get(0);
    SolrInputField iter = outDoc.getField("iter");
    Assert.assertNotNull("iter field is null", iter);
    Object iterVal = iter.getValue();
    Assert.assertTrue("iterVal is not a Collection",
        iterVal instanceof Collection);
    Assert.assertEquals("iterVal contents", values, iterVal);

  }

  //this format accepts a 1:1 mapping of the json format and javabin format
  @SuppressWarnings({"unchecked"})
  public void testStreamableInputDocFormat() throws IOException {
    @SuppressWarnings({"rawtypes"})
    Map m = Utils.makeMap("id","1","desc" ,"The desc 1");
    m.put(CHILDDOC, (MapWriter) ew -> {
      ew.put("id","1.1");
      ew.put("desc" ,"The desc 1.1");
      ew.put(CHILDDOC, (IteratorWriter) iw -> {
        iw.add(Utils.makeMap("id", "1.1.1","desc","The desc 1.1.1"));
        iw.add((MapWriter) ew1 -> {
          ew1.put("id", "1.1.2");
          ew1.put("desc", "The desc 1.1.2");
        });
      });
    });
    MapWriter m2 = ew -> {
      ew.put("id", "2");
      ew.put("des", "The desc 2");
    };

    @SuppressWarnings({"rawtypes"})
    List l = new ArrayList();
    l.add(m);
    l.add(m2);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new JavaBinCodec().marshal(l.iterator(), baos);

    List<SolrInputDocument>  l2 = new ArrayList<>();

    new JavaBinUpdateRequestCodec().unmarshal(new ByteArrayInputStream(baos.toByteArray()), (document, req, commitWithin, override) -> l2.add(document));

   assertEquals(l2.get(0).getChildDocuments().size(), 1);

   Object o = Utils.fromJSONString(Utils.writeJson(l.get(0), new StringWriter(), true).toString());
   Object cdoc = Utils.getObjectByPath(o, false, CHILDDOC);
   assertEquals(Utils.writeJson(cdoc, new StringWriter(), true).toString(),
       Utils.writeJson(l2.get(0).getChildDocuments().get(0) ,new StringWriter(), true).toString());

  }


  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testBackCompat4_5() throws IOException {

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.deleteById("*:*");
    updateRequest.deleteById("id:5");
    updateRequest.deleteByQuery("2*");
    updateRequest.deleteByQuery("1*");
    updateRequest.setParam("a", "b");
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1);
    doc.addField("desc", "one");
    doc.addField("desc", "1");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 2);
    doc.addField("desc", "two");
    doc.addField("desc", "2");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 3);
    doc.addField("desc", "three");
    doc.addField("desc", "3");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    Collection<String> foobar = new HashSet<>();
    foobar.add("baz1");
    foobar.add("baz2");
    doc.addField("foobar", foobar);
    updateRequest.add(doc);

    updateRequest.deleteById("2");
    updateRequest.deleteByQuery("id:3");


    InputStream is = getClass().getResourceAsStream("/solrj/updateReq_4_5.bin");
    assertNotNull("updateReq_4_5.bin was not found", is);
    UpdateRequest updateUnmarshalled = new JavaBinUpdateRequestCodec().unmarshal(is, (document, req, commitWithin, override) -> {
      if (commitWithin == null) {
        req.add(document);
      }
      System.err.println("Doc" + document + " ,commitWithin:" + commitWithin + " , override:" + override);
    });

    System.err.println(updateUnmarshalled.getDocumentsMap());
    System.err.println(updateUnmarshalled.getDocuments());

    for (int i = 0; i < updateRequest.getDocuments().size(); i++) {
      SolrInputDocument inDoc = updateRequest.getDocuments().get(i);
      SolrInputDocument outDoc = updateUnmarshalled.getDocuments().get(i);
      compareDocs("doc#" + i, inDoc, outDoc);
    }
    Assert.assertEquals(updateUnmarshalled.getDeleteById().get(0),
        updateRequest.getDeleteById().get(0));
    Assert.assertEquals(updateUnmarshalled.getDeleteQuery().get(0),
        updateRequest.getDeleteQuery().get(0));

    assertEquals("b", updateUnmarshalled.getParams().get("a"));
    is.close();
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private void compareDocs(String m,
                           SolrInputDocument expectedDoc,
                           SolrInputDocument actualDoc) {

    for (String s : expectedDoc.getFieldNames()) {
      SolrInputField expectedField = expectedDoc.getField(s);
      SolrInputField actualField = actualDoc.getField(s);
      Object expectedVal = expectedField.getValue();
      Object actualVal = actualField.getValue();
      if (expectedVal instanceof Set &&
          actualVal instanceof Collection) {
        // unmarshaled documents never contain Sets, they are just a 
        // List in an arbitrary order based on what the iterator of 
        // hte original Set returned, so we need a comparison that is 
        // order agnostic.
        actualVal = new HashSet((Collection) actualVal);
        m += " (Set comparison)";
      }

      Assert.assertEquals(m + " diff values for field: " + s,
          expectedVal, actualVal);
    }
  }

}
