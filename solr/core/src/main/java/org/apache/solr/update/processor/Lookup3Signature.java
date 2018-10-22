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
package org.apache.solr.update.processor;
import org.apache.solr.common.util.Hash;

public class Lookup3Signature extends Signature {

  protected long hash;

  public Lookup3Signature() {
  }

  @Override
  public void add(String content) {
    hash = Hash.lookup3ycs64(content,0,content.length(),hash);
  }

  @Override
  public byte[] getSignature() {
    return new byte[]{(byte)(hash>>56),(byte)(hash>>48),(byte)(hash>>40),(byte)(hash>>32),(byte)(hash>>24),(byte)(hash>>16),(byte)(hash>>8),(byte)(hash>>0)};
  }
}