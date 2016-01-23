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

package org.apache.solr.analysis;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;

public class TestDelimitedPayloadTokenFilterFactory extends BaseTokenTestCase {

  public void testEncoder() throws Exception {
    Map<String,String> args = new HashMap<String, String>();
    args.put(DelimitedPayloadTokenFilterFactory.ENCODER_ATTR, "float");
    DelimitedPayloadTokenFilterFactory factory = new DelimitedPayloadTokenFilterFactory();
    factory.init(args);
    ResourceLoader loader = new SolrResourceLoader(null, null);
    factory.inform(loader);

    TokenStream input = new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader("the|0.1 quick|0.1 red|0.1"));
    DelimitedPayloadTokenFilter tf = factory.create(input);
    while (tf.incrementToken()){
      PayloadAttribute payAttr = tf.getAttribute(PayloadAttribute.class);
      assertTrue("payAttr is null and it shouldn't be", payAttr != null);
      byte[] payData = payAttr.getPayload().getData();
      assertTrue("payData is null and it shouldn't be", payData != null);
      assertTrue("payData is null and it shouldn't be", payData != null);
      float payFloat = PayloadHelper.decodeFloat(payData);
      assertTrue(payFloat + " does not equal: " + 0.1f, payFloat == 0.1f);
    }
  }

  public void testDelim() throws Exception {
    Map<String,String> args = new HashMap<String, String>();
    args.put(DelimitedPayloadTokenFilterFactory.ENCODER_ATTR, FloatEncoder.class.getName());
    args.put(DelimitedPayloadTokenFilterFactory.DELIMITER_ATTR, "*");
    DelimitedPayloadTokenFilterFactory factory = new DelimitedPayloadTokenFilterFactory();
    factory.init(args);
    ResourceLoader loader = new SolrResourceLoader(null, null);
    factory.inform(loader);

    TokenStream input = new WhitespaceTokenizer(DEFAULT_VERSION, new StringReader("the*0.1 quick*0.1 red*0.1"));
    DelimitedPayloadTokenFilter tf = factory.create(input);
    while (tf.incrementToken()){
      PayloadAttribute payAttr = tf.getAttribute(PayloadAttribute.class);
      assertTrue("payAttr is null and it shouldn't be", payAttr != null);
      byte[] payData = payAttr.getPayload().getData();
      assertTrue("payData is null and it shouldn't be", payData != null);
      float payFloat = PayloadHelper.decodeFloat(payData);
      assertTrue(payFloat + " does not equal: " + 0.1f, payFloat == 0.1f);
    }
  }
}

