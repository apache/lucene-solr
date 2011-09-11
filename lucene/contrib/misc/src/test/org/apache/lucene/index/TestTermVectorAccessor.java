package org.apache.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

public class TestTermVectorAccessor extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    doc.add(newField("a", "a b a c a d a e a f a g a h a", customType));
    doc.add(newField("b", "a b c b d b e b f b g b h b", customType));
    doc.add(newField("c", "a c b c d c e c f c g c h c", customType));
    iw.addDocument(doc);

    doc = new Document();
    FieldType customType2 = new FieldType(TextField.TYPE_UNSTORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorPositions(true);
    doc.add(newField("a", "a b a c a d a e a f a g a h a", customType2));
    doc.add(newField("b", "a b c b d b e b f b g b h b", customType2));
    doc.add(newField("c", "a c b c d c e c f c g c h c", customType2));
    iw.addDocument(doc);

    doc = new Document();
    FieldType customType3 = new FieldType(TextField.TYPE_UNSTORED);
    customType3.setStoreTermVectors(true);
    doc.add(newField("a", "a b a c a d a e a f a g a h a", customType3));
    doc.add(newField("b", "a b c b d b e b f b g b h b", customType3));
    doc.add(newField("c", "a c b c d c e c f c g c h c", customType3));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(newField("a", "a b a c a d a e a f a g a h a", TextField.TYPE_UNSTORED));
    doc.add(newField("b", "a b c b d b e b f b g b h b", TextField.TYPE_UNSTORED));
    doc.add(newField("c", "a c b c d c e c f c g c h c", TextField.TYPE_UNSTORED));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(newField("a", "a b a c a d a e a f a g a h a", customType));
    doc.add(newField("b", "a b c b d b e b f b g b h b", TextField.TYPE_UNSTORED));
    doc.add(newField("c", "a c b c d c e c f c g c h c", customType3));
    iw.addDocument(doc);

    iw.close();

    IndexReader ir = IndexReader.open(dir, false);

    TermVectorAccessor accessor = new TermVectorAccessor();

    ParallelArrayTermVectorMapper mapper;
    TermFreqVector tfv;

    for (int i = 0; i < ir.maxDoc(); i++) {

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "a", mapper);
      tfv = mapper.materializeVector();
      assertEquals("doc " + i, "a", tfv.getTerms()[0].utf8ToString());
      assertEquals("doc " + i, 8, tfv.getTermFrequencies()[0]);

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "b", mapper);
      tfv = mapper.materializeVector();
      assertEquals("doc " + i, 8, tfv.getTermFrequencies().length);
      assertEquals("doc " + i, "b", tfv.getTerms()[1].utf8ToString());
      assertEquals("doc " + i, 7, tfv.getTermFrequencies()[1]);

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "c", mapper);
      tfv = mapper.materializeVector();
      assertEquals("doc " + i, 8, tfv.getTermFrequencies().length);
      assertEquals("doc " + i, "c", tfv.getTerms()[2].utf8ToString());
      assertEquals("doc " + i, 7, tfv.getTermFrequencies()[2]);

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "q", mapper);
      tfv = mapper.materializeVector();
      assertNull("doc " + i, tfv);

    }

    ir.close();

    dir.close();


  }

}
