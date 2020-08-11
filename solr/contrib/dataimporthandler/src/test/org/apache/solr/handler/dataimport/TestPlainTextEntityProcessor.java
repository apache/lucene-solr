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
package org.apache.solr.handler.dataimport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import org.apache.solr.common.util.Utils;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test for PlainTextEntityProcessor
 *
 *
 * @see org.apache.solr.handler.dataimport.PlainTextEntityProcessor
 * @since solr 1.4
 */
public class TestPlainTextEntityProcessor extends AbstractDataImportHandlerTestCase {
  @Test
  public void testSimple() throws IOException {
    DataImporter di = new DataImporter();
    di.loadAndInit(DATA_CONFIG);
    redirectTempProperties(di);

    TestDocBuilder.SolrWriterImpl sw = new TestDocBuilder.SolrWriterImpl();
    @SuppressWarnings({"unchecked"})
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    di.runCmd(rp, sw);
    assertEquals(DS.s, sw.docs.get(0).getFieldValue("x"));
  }

  static class BlobImpl implements Blob{
    private final byte[] bytes;

    BlobImpl(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long length() throws SQLException {
      return 0;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
      return bytes;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
      return new ByteArrayInputStream(bytes);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
      return 0;
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
      return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
      return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
      return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
      return null;
    }

    @Override
    public void truncate(long len) throws SQLException {

    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
      return new ByteArrayInputStream(bytes);
    }
  }

  @Test
  public void testSimple2() throws IOException {
    DataImporter di = new DataImporter();
    MockDataSource.setIterator("select id, name, blob_field from lw_table4", Collections.singletonList(Utils.makeMap("blob_field",new BlobImpl(DS.s.getBytes(UTF_8)) ) ).iterator());

    String dc =

        " <dataConfig>" +
            "<dataSource name=\"ds1\" type=\"MockDataSource\"/>\n" +
        " <!-- dataSource for FieldReaderDataSource -->\n" +
        " <dataSource dataField=\"root.blob_field\" name=\"fr\" type=\"FieldReaderDataSource\"/>\n" +
        "\n" +
        " <document name=\"items\">\n" +
        "   <entity dataSource=\"ds1\" name=\"root\" pk=\"id\"  query=\"select id, name, blob_field from lw_table4\" transformer=\"TemplateTransformer\">\n" +
        "           <field column=\"id\" name=\"id\"/>\n" +
        "\n" +
        "        <entity dataField=\"root.blob_field\" dataSource=\"fr\" format=\"text\" name=\"n1\" processor=\"PlainTextEntityProcessor\" url=\"blob_field\">\n" +
        "                       <field column=\"plainText\" name=\"plainText\"/>\n" +
        "           </entity>\n" +
        "\n" +
        "   </entity>\n" +
        " </document>\n" +
        "</dataConfig>";
    System.out.println(dc);
    di.loadAndInit(dc);
    redirectTempProperties(di);

    TestDocBuilder.SolrWriterImpl sw = new TestDocBuilder.SolrWriterImpl();
    @SuppressWarnings({"unchecked"})
    RequestInfo rp = new RequestInfo(null, createMap("command", "full-import"), null);
    di.runCmd(rp, sw);
    assertEquals(DS.s, sw.docs.get(0).getFieldValue("plainText"));
  }


  @SuppressWarnings({"rawtypes"})
  public static class DS extends DataSource {
    static String s = "hello world";

    @Override
    public void init(Context context, Properties initProps) {

    }

    @Override
    public Object getData(String query) {

      return new StringReader(s);
    }

    @Override
    public void close() {

    }
  }

  static String DATA_CONFIG = "<dataConfig>\n" +
          "\t<dataSource type=\"TestPlainTextEntityProcessor$DS\" />\n" +
          "\t<document>\n" +
          "\t\t<entity processor=\"PlainTextEntityProcessor\" name=\"x\" query=\"x\">\n" +
          "\t\t\t<field column=\"plainText\" name=\"x\" />\n" +
          "\t\t</entity>\n" +
          "\t</document>\n" +
          "</dataConfig>";
}
