package org.apache.solr.common.cloud;

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

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.Hash;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

public class Hasher {
  public int hash(AddUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
  
  public int hash(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
}
