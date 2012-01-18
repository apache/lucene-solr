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
package org.apache.solr.client.solrj.request;

import junit.framework.Assert;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.FastInputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * Test for UpdateRequestCodec
 *
 * @since solr 1.4
 *
 * @see org.apache.solr.client.solrj.request.UpdateRequest
 */
public class TestUpdateRequestCodec extends LuceneTestCase {

  @Test
  public void simple() throws IOException {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.deleteById("*:*");
    updateRequest.deleteById("id:5");
    updateRequest.deleteByQuery("2*");
    updateRequest.deleteByQuery("1*");
    updateRequest.setParam("a", "b");
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1);
    doc.addField("desc", "one", 2.0f);
    doc.addField("desc", "1");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 2);
    doc.setDocumentBoost(10.0f);
    doc.addField("desc", "two", 3.0f);
    doc.addField("desc", "2");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 3);
    doc.addField("desc", "three", 3.0f);
    doc.addField("desc", "3");
    updateRequest.add(doc);

    doc = new SolrInputDocument();
    Collection<String> foobar = new HashSet<String>();
    foobar.add("baz1");
    foobar.add("baz2");
    doc.addField("foobar",foobar);
    updateRequest.add(doc);

//    updateRequest.setWaitFlush(true);
    updateRequest.deleteById("2");
    updateRequest.deleteByQuery("id:3");
    JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    codec.marshal(updateRequest, baos);
    final List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    JavaBinUpdateRequestCodec.StreamingUpdateHandler handler = new JavaBinUpdateRequestCodec.StreamingUpdateHandler() {
      public void update(SolrInputDocument document, UpdateRequest req) {
        Assert.assertNotNull(req.getParams());
        docs.add(document);
      }
    };

    UpdateRequest updateUnmarshalled = codec.unmarshal(new ByteArrayInputStream(baos.toByteArray()) ,handler);
    Assert.assertNull(updateUnmarshalled.getDocuments());
    for (SolrInputDocument document : docs) {
      updateUnmarshalled.add(document);
    }
    for (int i = 0; i < updateRequest.getDocuments().size(); i++) {
      SolrInputDocument inDoc = updateRequest.getDocuments().get(i);
      SolrInputDocument outDoc = updateUnmarshalled.getDocuments().get(i);
      compareDocs("doc#"+i, inDoc, outDoc);
    }
    Assert.assertEquals(updateUnmarshalled.getDeleteById().get(0) , 
                        updateRequest.getDeleteById().get(0));
    Assert.assertEquals(updateUnmarshalled.getDeleteQuery().get(0) , 
                        updateRequest.getDeleteQuery().get(0));

    assertEquals("b", updateUnmarshalled.getParams().get("a"));
  }

  @Test
  public void testIteratable() throws IOException {
    final List<String> values = new ArrayList<String>();
    values.add("iterItem1");
    values.add("iterItem2");

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.deleteByQuery("*:*");

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 1);
    doc.addField("desc", "one", 2.0f);
    // imagine someone adding a custom Bean that implements Iterable 
    // but is not a Collection
    doc.addField("iter", new Iterable<String>() { 
        public Iterator<String> iterator() { return values.iterator(); } 
      });
    doc.addField("desc", "1");
    updateRequest.add(doc);

    JavaBinUpdateRequestCodec codec = new JavaBinUpdateRequestCodec();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    codec.marshal(updateRequest, baos);
    final List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    JavaBinUpdateRequestCodec.StreamingUpdateHandler handler = new JavaBinUpdateRequestCodec.StreamingUpdateHandler() {
      public void update(SolrInputDocument document, UpdateRequest req) {
        Assert.assertNotNull(req.getParams());
        docs.add(document);
      }
    };

    UpdateRequest updateUnmarshalled = codec.unmarshal(new ByteArrayInputStream(baos.toByteArray()) ,handler);
    Assert.assertNull(updateUnmarshalled.getDocuments());
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



  private void compareDocs(String m, 
                           SolrInputDocument expectedDoc, 
                           SolrInputDocument actualDoc) {
    Assert.assertEquals(expectedDoc.getDocumentBoost(), 
                        actualDoc.getDocumentBoost());

    for (String s : expectedDoc.getFieldNames()) {
      SolrInputField expectedField = expectedDoc.getField(s);
      SolrInputField actualField = actualDoc.getField(s);
      Assert.assertEquals(m + ": diff boosts for field: " + s,
                          expectedField.getBoost(), actualField.getBoost());
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
