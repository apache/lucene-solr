package org.apache.lucene.index;

import junit.framework.TestCase;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.util.Collections;
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


public class TestTermVectorAccessor extends TestCase {

  public void test() throws Exception {

    Directory dir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(Collections.EMPTY_SET), true);

    Document doc;

    doc = new Document();
    doc.add(new Field("a", "a b a c a d a e a f a g a h a", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("b", "a b c b d b e b f b g b h b", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("c", "a c b c d c e c f c g c h c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("a", "a b a c a d a e a f a g a h a", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
    doc.add(new Field("b", "a b c b d b e b f b g b h b", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
    doc.add(new Field("c", "a c b c d c e c f c g c h c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("a", "a b a c a d a e a f a g a h a", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
    doc.add(new Field("b", "a b c b d b e b f b g b h b", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
    doc.add(new Field("c", "a c b c d c e c f c g c h c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("a", "a b a c a d a e a f a g a h a", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
    doc.add(new Field("b", "a b c b d b e b f b g b h b", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
    doc.add(new Field("c", "a c b c d c e c f c g c h c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("a", "a b a c a d a e a f a g a h a", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("b", "a b c b d b e b f b g b h b", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
    doc.add(new Field("c", "a c b c d c e c f c g c h c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
    iw.addDocument(doc);

    iw.close();

    IndexReader ir = IndexReader.open(dir);

    TermVectorAccessor accessor = new TermVectorAccessor();

    ParallelArrayTermVectorMapper mapper;
    TermFreqVector tfv;

    for (int i = 0; i < ir.maxDoc(); i++) {

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "a", mapper);
      tfv = mapper.materializeVector();
      assertEquals("doc " + i, "a", tfv.getTerms()[0]);
      assertEquals("doc " + i, 8, tfv.getTermFrequencies()[0]);

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "b", mapper);
      tfv = mapper.materializeVector();
      assertEquals("doc " + i, 8, tfv.getTermFrequencies().length);
      assertEquals("doc " + i, "b", tfv.getTerms()[1]);
      assertEquals("doc " + i, 7, tfv.getTermFrequencies()[1]);

      mapper = new ParallelArrayTermVectorMapper();
      accessor.accept(ir, i, "c", mapper);
      tfv = mapper.materializeVector();
      assertEquals("doc " + i, 8, tfv.getTermFrequencies().length);
      assertEquals("doc " + i, "c", tfv.getTerms()[2]);
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
