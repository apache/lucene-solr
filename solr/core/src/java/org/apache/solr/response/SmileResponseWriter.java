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

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

public class SmileResponseWriter extends BinaryResponseWriter {

  @Override
  public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    new SmileWriter(out, request, response).writeResponse();
  }

  @Override
  public void init(NamedList args) {

  }
  //smile format is an equivalent of JSON format . So we extend JSONWriter and override the relevant methods

  public static class SmileWriter extends JSONWriter {
    protected final SmileGenerator gen;
    protected final OutputStream out;

    public SmileWriter(OutputStream out, SolrQueryRequest req, SolrQueryResponse rsp) {
      super(null, req, rsp);
      this.out = out;
      SmileFactory smileFactory = new SmileFactory();
      smileFactory.enable(SmileGenerator.Feature.CHECK_SHARED_NAMES);
      try {
        gen = smileFactory.createGenerator(this.out, null);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }


    @Override
    public void writeResponse() throws IOException {
      //we always write header , it is just 4 bytes and not worth optimizing
      gen.writeHeader();
      super.writeNamedList(null, rsp.getValues());
      gen.close();
    }

    @Override
    protected void writeNumber(String name, Number val) throws IOException {
      if (val instanceof Integer) {
        gen.writeNumber(val.intValue());
      } else if (val instanceof Long) {
        gen.writeNumber(val.longValue());
      } else if (val instanceof Float) {
        gen.writeNumber(val.floatValue());
      } else if (val instanceof Double) {
        gen.writeNumber(val.floatValue());
      } else if (val instanceof Short) {
        gen.writeNumber(val.shortValue());
      } else if (val instanceof Byte) {
        gen.writeNumber(val.byteValue());
      } else if (val instanceof BigInteger) {
        gen.writeNumber((BigInteger) val);
      } else if (val instanceof BigDecimal) {
        gen.writeNumber((BigDecimal) val);
      } else {
        gen.writeString(val.getClass().getName() + ':' + val.toString());
        // default... for debugging only
      }
    }

    @Override
    public void writeBool(String name, Boolean val) throws IOException {
      gen.writeBoolean(val);
    }

    @Override
    public void writeNull(String name) throws IOException {
      gen.writeNull();
    }

    @Override
    public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
      gen.writeString(val);
    }

    @Override
    public void writeLong(String name, long val) throws IOException {
      gen.writeNumber(val);
    }

    @Override
    public void writeInt(String name, int val) throws IOException {
      gen.writeNumber(val);
    }

    @Override
    public void writeBool(String name, boolean val) throws IOException {
      gen.writeBoolean(val);
    }

    @Override
    public void writeFloat(String name, float val) throws IOException {
      gen.writeNumber(val);
    }

    @Override
    public void writeArrayCloser() throws IOException {
      gen.writeEndArray();
    }

    @Override
    public void writeArraySeparator() throws IOException {
      //do nothing
    }

    @Override
    public void writeArrayOpener(int size) throws IOException, IllegalArgumentException {
      gen.writeStartArray();
    }

    @Override
    public void writeMapCloser() throws IOException {
      gen.writeEndObject();
    }

    @Override
    public void writeMapSeparator() throws IOException {
      //do nothing
    }

    @Override
    public void writeMapOpener(int size) throws IOException, IllegalArgumentException {
      gen.writeStartObject();
    }

    @Override
    protected void writeKey(String fname, boolean needsEscaping) throws IOException {
      gen.writeFieldName(fname);
    }

    @Override
    public void writeByteArr(String name, byte[] buf, int offset, int len) throws IOException {
      gen.writeBinary(buf, offset, len);

    }

    @Override
    public void setLevel(int level) {
      //do nothing
    }

    @Override
    public int level() {
      return 0;
    }

    @Override
    public void indent() throws IOException {
      //do nothing
    }

    @Override
    public void indent(int lev) throws IOException {
      //do nothing
    }

    @Override
    public int incLevel() {
      return 0;
    }

    @Override
    public int decLevel() {
      return 0;
    }

    @Override
    public void close() throws IOException {
      gen.close();
    }
  }
}
