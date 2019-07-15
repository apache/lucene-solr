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
package org.apache.solr.schema;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;

public class DateFieldTest extends SolrTestCaseJ4 {
  private final String testInstanceDir = TEST_HOME() + File.separator + "collection1";
  private final String testConfHome = testInstanceDir + File.separator + "conf"+ File.separator;
  private FieldType f = null;

  @Override
  public void setUp()  throws Exception {
    super.setUp();
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    SolrConfig config = new SolrConfig
        (new SolrResourceLoader(Paths.get(testInstanceDir)), testConfHome + "solrconfig.xml", null);
    IndexSchema schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema.xml", config);
    f = Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)
      ? new DatePointField() : new TrieDateField();
    f.init(schema, Collections.<String,String>emptyMap());
  }

  // NOTE: Many other tests were moved to DateMathParserTest

  public void testCreateField() {
    int props = FieldProperties.INDEXED ^ FieldProperties.STORED;
    SchemaField sf = new SchemaField( "test", f, props, null );
    // String
    IndexableField out = f.createField(sf, "1995-12-31T23:59:59Z" );
    assertEquals(820454399000L, ((Date) f.toObject( out )).getTime() );
    // Date obj
    out = f.createField(sf, new Date(820454399000L) );
    assertEquals(820454399000L, ((Date) f.toObject( out )).getTime() );
    // Date math
    out = f.createField(sf, "1995-12-31T23:59:59.99Z+5MINUTES");
    assertEquals(820454699990L, ((Date) f.toObject( out )).getTime() );
  }

  public void testToNativeType() {
    FieldType ft = new TrieDateField();
    ByteArrayUtf8CharSequence charSequence = new ByteArrayUtf8CharSequence("1995-12-31T23:59:59Z");

    Date val = (Date) ft.toNativeType(charSequence);
    assertEquals("1995-12-31T23:59:59Z", val.toInstant().toString());
  }

}
