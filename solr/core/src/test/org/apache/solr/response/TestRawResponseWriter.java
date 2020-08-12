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
package org.apache.solr.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.util.ContentStreamBase.ByteArrayStream;
import org.apache.solr.common.util.ContentStreamBase.StringStream;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.AfterClass;

/**
 * Tests the {@link RawResponseWriter} behavior, in particular when dealing with "base" writer
 */
public class TestRawResponseWriter extends SolrTestCaseJ4 {
  
  private static RawResponseWriter writerXmlBase;
  private static RawResponseWriter writerJsonBase;
  private static RawResponseWriter writerBinBase;
  private static RawResponseWriter writerNoBase;

  private static RawResponseWriter[] allWriters;

  @BeforeClass
  public static void setupCoreAndWriters() throws Exception {
    // we don't directly use this core or its config, we use
    // QueryResponseWriters' constructed programmatically,
    // but we do use this core for managing the life cycle of the requests
    // we spin up.
    initCore("solrconfig.xml","schema.xml");

    writerNoBase = newRawResponseWriter(null); /* defaults to standard writer as base */
    writerXmlBase = newRawResponseWriter("xml");
    writerJsonBase = newRawResponseWriter("json");
    writerBinBase = newRawResponseWriter("javabin");

    allWriters = new RawResponseWriter[] { 
      writerXmlBase, writerJsonBase, writerBinBase, writerNoBase 
    };
  }

  @AfterClass
  public static void cleanupWriters() throws Exception {
    writerXmlBase = null;
    writerJsonBase = null;
    writerBinBase = null;
    writerNoBase = null;

    allWriters = null;
  }

  /**
   * Regardless of base writer, the bytes in should be the same as the bytes out 
   * when response is a raw ContentStream written to an OutputStream
   */
  public void testRawBinaryContentStream()  throws IOException {
    SolrQueryResponse rsp = new SolrQueryResponse();
    byte[] data = new byte[TestUtil.nextInt(random(), 10, 2048)];
    random().nextBytes(data);
    ByteArrayStream stream = new ByteArrayStream(data, TestUtil.randomUnicodeString(random()));

    stream.setContentType(TestUtil.randomSimpleString(random()));
    rsp.add(RawResponseWriter.CONTENT, stream);
    
    for (RawResponseWriter writer : allWriters) {
      assertEquals(stream.getContentType(), writer.getContentType(req(), rsp));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      writer.write(out, req(), rsp);
      assertArrayEquals(data, out.toByteArray());
    }
  }

  /**
   * Regardless of base writer, the String in should be the same as the String out 
   * when response is a raw ContentStream written to a Writer (or OutputStream)
   */
  public void testRawStringContentStream()  throws IOException {
    SolrQueryResponse rsp = new SolrQueryResponse();
    String data = TestUtil.randomUnicodeString(random());
    StringStream stream = new StringStream(data);

    stream.setContentType(TestUtil.randomSimpleString(random()));
    rsp.add(RawResponseWriter.CONTENT, stream);
    
    for (RawResponseWriter writer : allWriters) {
      assertEquals(stream.getContentType(), writer.getContentType(req(), rsp));

      // we should have the same string if we use a Writer
      StringWriter sout = new StringWriter();
      writer.write(sout, req(), rsp);
      assertEquals(data, sout.toString());

      // we should have UTF-8 Bytes if we use an OutputStream
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      writer.write(bout, req(), rsp);
      assertEquals(data, bout.toString(StandardCharsets.UTF_8.toString()));
    }
  }

  /**
   * When no real ContentStream is specified, each base writer should be used for formatting
   */
  public void testStructuredDataViaBaseWriters() throws IOException {
    SolrQueryResponse rsp = new SolrQueryResponse();
    // Don't send a ContentStream back, this will fall back to the configured base writer.
    // But abuse the CONTENT key to ensure writer is also checking type
    rsp.add(RawResponseWriter.CONTENT, "test");
    rsp.add("foo", "bar");

    // check Content-Type against each writer 
    assertEquals("application/xml; charset=UTF-8", writerNoBase.getContentType(req(), rsp));
    assertEquals("application/xml; charset=UTF-8", writerXmlBase.getContentType(req(), rsp));
    assertEquals("application/json; charset=UTF-8", writerJsonBase.getContentType(req(), rsp));
    assertEquals("application/octet-stream",  writerBinBase.getContentType(req(), rsp));

    // check response against each writer

    // xml & none (default behavior same as XML)
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<response>\n" +
        "\n" +
        "<str name=\"content\">test</str>\n" +
        "<str name=\"foo\">bar</str>\n" +
        "</response>\n";
    StringWriter xmlSout = new StringWriter();
    writerXmlBase.write(xmlSout, req(), rsp);
    assertEquals(xml, xmlSout.toString());
    ByteArrayOutputStream xmlBout = new ByteArrayOutputStream();
    writerXmlBase.write(xmlBout, req(), rsp);
    assertEquals(xml, xmlBout.toString(StandardCharsets.UTF_8.toString()));
    //
    StringWriter noneSout = new StringWriter();
    writerNoBase.write(noneSout, req(), rsp);
    assertEquals(xml, noneSout.toString());
    ByteArrayOutputStream noneBout = new ByteArrayOutputStream();
    writerNoBase.write(noneBout, req(), rsp);
    assertEquals(xml, noneBout.toString(StandardCharsets.UTF_8.toString()));

    // json
    String json = "{\n" +
        "  \"content\":\"test\",\n" +
        "  \"foo\":\"bar\"}\n";
    StringWriter jsonSout = new StringWriter();
    writerJsonBase.write(jsonSout, req(), rsp);
    assertEquals(json, jsonSout.toString());
    ByteArrayOutputStream jsonBout = new ByteArrayOutputStream();
    writerJsonBase.write(jsonBout, req(), rsp);
    assertEquals(json, jsonBout.toString(StandardCharsets.UTF_8.toString()));

    // javabin
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    writerBinBase.write(bytes, req(), rsp);
    BinaryResponseParser parser = new BinaryResponseParser();
    NamedList<Object> out = parser.processResponse
      (new ByteArrayInputStream(bytes.toByteArray()), /* encoding irrelevant */ null);
    assertEquals(RawResponseWriter.CONTENT, out.getName(0));
    assertEquals("test", out.getVal(0));
    assertEquals("foo", out.getName(1));
    assertEquals("bar", out.getVal(1));

  }

  /**
   * Generates a new {@link RawResponseWriter} wrapping the specified baseWriter name 
   * (which much either be an implicitly defined response writer, or one explicitly 
   * configured in solrconfig.xml)
   *
   * @param baseWriter null or the name of a valid base writer
   */
  @SuppressWarnings({"unchecked"})
  private static RawResponseWriter newRawResponseWriter(String baseWriter) {
    RawResponseWriter writer = new RawResponseWriter();
    @SuppressWarnings({"rawtypes"})
    NamedList initArgs = new NamedList<Object>();
    if (null != baseWriter) {
      initArgs.add("base", baseWriter);
    }
    writer.init(initArgs);
    return writer;
  }
  
}
