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

package org.apache.lucene.queries.mlt;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.core.Is.is;

public class MoreLikeThisParametersTest extends LuceneTestCase {
  private MoreLikeThisParameters toTest = new MoreLikeThisParameters();

  public void testMLTParameters_setFieldNames_shouldRemoveBoosts() {
    String[] fieldNames = new String[]{"field1", "field2^5.0", "field3^1.0", "field4"};

    toTest.setFieldNames(fieldNames);

    assertThat(toTest.getFieldNames(), is(new String[]{"field1", "field2", "field3", "field4"}));
  }

  public void testMLTParameters_fieldNamesNotInitialised_shouldFetchThemFromIndex() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    Document doc = new Document();
    doc.add(newTextField("field1", "lucene", Field.Store.YES));
    doc.add(newTextField("field2", "more like this", Field.Store.YES));
    writer.addDocument(doc);
    
    IndexReader reader = writer.getReader();
    writer.close();

    toTest.getFieldNamesOrInit(reader);

    assertThat(toTest.getFieldNames(), is(new String[]{"field1", "field2"}));

    reader.close();
    directory.close();
  }

  public void testMLTParameters_describeParams_shouldReturnParamsDescriptionString() {
    String[] fieldNames = new String[]{"field1", "field2^5.0", "field3^1.0", "field4"};
    toTest.setFieldNames(fieldNames);

    String paramsDescription = toTest.describeParams();

    assertThat(paramsDescription,
        is("\tmaxQueryTerms  : 25\n" +
            "\tminWordLen     : 0\n" +
            "\tmaxWordLen     : 0\n" +
            "\tfieldNames     : field1, field2, field3, field4\n" +
            "\tboost          : false\n" +
            "\tminTermFreq    : 2\n" +
            "\tminDocFreq     : 5\n"));
  }

  public void testMLTParameters_setBoostFields_shouldParseBoosts() {
    String[] fieldNames = new String[]{"field1", "field2^2.0", "field3^3.0", "field4"};
    MoreLikeThisParameters.BoostProperties boostConfiguration = toTest.getBoostConfiguration();

    for (String fieldWithBoost : fieldNames) {
      boostConfiguration.addFieldWithBoost(fieldWithBoost);
    }

    assertThat(boostConfiguration.getFieldBoost("field1"), is(1.0F));
    assertThat(boostConfiguration.getFieldBoost("field2"), is(2.0F));
    assertThat(boostConfiguration.getFieldBoost("field3"), is(3.0F));
    assertThat(boostConfiguration.getFieldBoost("field4"), is(1.0F));
  }

  public void testMLTParameters_noBoostConfiguration_shouldReturnDefaultBoost() {
    MoreLikeThisParameters.BoostProperties boostConfiguration = toTest.getBoostConfiguration();

    assertThat(boostConfiguration.getFieldBoost("field1"), is(1.0F));
    assertThat(boostConfiguration.getFieldBoost("field2"), is(1.0F));
    assertThat(boostConfiguration.getFieldBoost("field3"), is(1.0F));
    assertThat(boostConfiguration.getFieldBoost("field4"), is(1.0F));

    boostConfiguration.setBoostFactor(5.0f);

    assertThat(boostConfiguration.getFieldBoost("field1"), is(5.0F));
    assertThat(boostConfiguration.getFieldBoost("field2"), is(5.0F));
    assertThat(boostConfiguration.getFieldBoost("field3"), is(5.0F));
    assertThat(boostConfiguration.getFieldBoost("field4"), is(5.0F));
  }
}
