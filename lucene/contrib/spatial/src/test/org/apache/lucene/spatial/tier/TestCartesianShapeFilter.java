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
package org.apache.lucene.spatial.tier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;

import junit.framework.TestCase;
/**
 * 
 * Test for {@link CartesianShapeFilter}
 *
 */
public class TestCartesianShapeFilter extends TestCase {

  public void testSerializable() throws IOException {
    CartesianShapeFilter filter = new CartesianShapeFilter(new Shape("1"),
        "test");
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(filter);
    } catch (NotSerializableException e) {
      fail("Filter should be serializable but raised a NotSerializableException ["+e.getMessage()+"]");
    }
  }

}
