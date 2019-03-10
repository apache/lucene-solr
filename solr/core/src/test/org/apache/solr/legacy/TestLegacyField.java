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
package org.apache.solr.legacy;

import java.io.StringReader;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCase;

public class TestLegacyField extends SolrTestCase {
  
  public void testLegacyDoubleField() throws Exception {
    Field fields[] = new Field[] {
        new LegacyDoubleField("foo", 5d, Field.Store.NO),
        new LegacyDoubleField("foo", 5d, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      field.setDoubleValue(6d); // ok
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
    
      assertEquals(6d, field.numericValue().doubleValue(), 0.0d);
    }
  }
  
  public void testLegacyFloatField() throws Exception {
    Field fields[] = new Field[] {
        new LegacyFloatField("foo", 5f, Field.Store.NO),
        new LegacyFloatField("foo", 5f, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      field.setFloatValue(6f); // ok
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(6f, field.numericValue().floatValue(), 0.0f);
    }
  }
  
  public void testLegacyIntField() throws Exception {
    Field fields[] = new Field[] {
        new LegacyIntField("foo", 5, Field.Store.NO),
        new LegacyIntField("foo", 5, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      field.setIntValue(6); // ok
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(6, field.numericValue().intValue());
    }
  }
  
  public void testLegacyLongField() throws Exception {
    Field fields[] = new Field[] {
        new LegacyLongField("foo", 5L, Field.Store.NO),
        new LegacyLongField("foo", 5L, Field.Store.YES)
    };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      field.setLongValue(6); // ok
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);
      
      assertEquals(6L, field.numericValue().longValue());
    }
  }
  
  private void trySetByteValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setByteValue((byte) 10);
    });
  }

  private void trySetBytesValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setBytesValue(new byte[] { 5, 5 });
    });
  }
  
  private void trySetBytesRefValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setBytesValue(new BytesRef("bogus"));
    });
  }
  
  private void trySetDoubleValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setDoubleValue(Double.MAX_VALUE);
    });
  }
  
  private void trySetIntValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setIntValue(Integer.MAX_VALUE);
    });
  }
  
  private void trySetLongValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setLongValue(Long.MAX_VALUE);
    });
  }
  
  private void trySetFloatValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setFloatValue(Float.MAX_VALUE);
    });
  }
  
  private void trySetReaderValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setReaderValue(new StringReader("BOO!"));
    });
  }
  
  private void trySetShortValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setShortValue(Short.MAX_VALUE);
    });
  }
  
  private void trySetStringValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setStringValue("BOO!");
    });
  }
  
  private void trySetTokenStreamValue(Field f) {
    expectThrows(IllegalArgumentException.class, () -> {
      f.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));
    });
  }
}
