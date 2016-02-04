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
package org.apache.solr.util;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.lucene.store.IndexOutput;

public class PropertiesOutputStream extends OutputStream {
  
  private IndexOutput out;

  public PropertiesOutputStream(IndexOutput out) {
    this.out = out;
  }
  
  @Override
  public void write(int b) throws IOException {
    out.writeByte((byte) b);
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    out.close();
  }
  
}
