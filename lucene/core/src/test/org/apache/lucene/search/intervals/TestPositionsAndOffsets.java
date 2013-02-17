package org.apache.lucene.search.intervals;
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

import java.io.IOException;

// We need to store offsets here, so don't use the following Codecs, which don't
// support them.
@SuppressCodecs({"MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom"})
public class TestPositionsAndOffsets extends IntervalTestBase {

  protected void addDocs(RandomIndexWriter writer) throws IOException {
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();
    doc.add(newField(
        "field",
        "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
            + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
        fieldType));
    writer.addDocument(doc);
  }

  public void testTermQueryOffsets() throws IOException {
    Query query = new TermQuery(new Term("field", "porridge"));
    checkIntervalOffsets(query, searcher, new int[][]{
        { 0, 6, 14, 26, 34, 47, 55, 164, 172, 184, 192 }
    });
  }

  public void testBooleanQueryOffsets() throws IOException {
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")),
        BooleanClause.Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "nine")),
        BooleanClause.Occur.MUST));
    checkIntervalOffsets(query,  searcher, new int[][]{
        { 0, 6, 14, 26, 34, 47, 55, 67, 71, 143, 147, 164, 172, 184, 192 }
    });
  }

}