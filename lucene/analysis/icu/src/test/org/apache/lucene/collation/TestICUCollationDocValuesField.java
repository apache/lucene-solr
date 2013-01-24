package org.apache.lucene.collation;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;

/**
 * trivial test of ICUCollationDocValuesField
 */
public class TestICUCollationDocValuesField extends LuceneTestCase {
  public void test() throws Exception {
    assumeFalse("3.x codec does not support docvalues", Codec.getDefault().getName().equals("Lucene3x"));
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field field = newField("field", "", StringField.TYPE_STORED);
    ICUCollationDocValuesField collationField = new ICUCollationDocValuesField("collated", Collator.getInstance(ULocale.ENGLISH));
    doc.add(field);
    doc.add(collationField);

    field.setStringValue("ABC");
    collationField.setStringValue("ABC");
    iw.addDocument(doc);
    
    field.setStringValue("abc");
    collationField.setStringValue("abc");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = newSearcher(ir);
    
    SortField sortField = new SortField("collated", SortField.Type.STRING);
    
    TopDocs td = is.search(new MatchAllDocsQuery(), 5, new Sort(sortField));
    assertEquals("abc", ir.document(td.scoreDocs[0].doc).get("field"));
    assertEquals("ABC", ir.document(td.scoreDocs[1].doc).get("field"));
    ir.close();
    dir.close();
  }
}
