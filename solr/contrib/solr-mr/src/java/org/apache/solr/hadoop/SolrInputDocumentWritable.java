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
package org.apache.solr.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;

public class SolrInputDocumentWritable implements Writable {
  private SolrInputDocument sid;

  public SolrInputDocumentWritable() {
  }

  public SolrInputDocumentWritable(SolrInputDocument sid) {
    this.sid = sid;
  }

  public SolrInputDocument getSolrInputDocument() {
    return sid;
  }

  @Override
  public String toString() {
    return sid.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    FastOutputStream daos = FastOutputStream.wrap(DataOutputOutputStream.constructOutputStream(out));
    codec.init(daos);
    try {
      codec.writeVal(sid);
    } finally {
      daos.flushBuffer();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    UnbufferedDataInputInputStream dis = new UnbufferedDataInputInputStream(in);
    sid = (SolrInputDocument)codec.readVal(dis);
  }

}
