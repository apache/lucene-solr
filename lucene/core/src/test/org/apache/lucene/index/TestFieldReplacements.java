package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldsUpdate.Operation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestFieldReplacements extends LuceneTestCase {
  private Directory dir;
  
  private static String[] fieldNames = null;
  private static String[][] fieldTokens = null;
  private static String loremIpsum = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, "
      + "sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. "
      + "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper suscipit lobortis "
      + "nisl ut aliquip ex ea commodo consequat. Duis autem vel eum iriure dolor in hendrerit "
      + "in vulputate velit esse molestie consequat, vel illum dolore eu feugiat nulla facilisis "
      + "at vero eros et accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      + "delenit augue duis dolore te feugait nulla facilisi. Nam liber tempor cum soluta nobis "
      + "eleifend option congue nihil imperdiet doming id quod mazim placerat facer possim assum. "
      + "Typi non habent claritatem insitam; est usus legentis in iis qui facit eorum claritatem. "
      + "Investigationes demonstraverunt lectores legere me lius quod ii legunt saepius. Claritas "
      + "est etiam processus dynamicus, qui sequitur mutationem consuetudium lectorum. Mirum est "
      + "notare quam littera gothica, quam nunc putamus parum claram, anteposuerit litterarum "
      + "formas humanitatis per seacula quarta decima et quinta decima. Eodem modo typi, qui nunc "
      + "nobis videntur parum clari, fiant sollemnes in futurum.";
  
  private final static boolean VERBOSE_FIELD_REPLACEMENTS = false;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    
    // init fields data structures
    int numFields = 4 + random().nextInt(4);
    fieldNames = new String[numFields];
    fieldTokens = new String[numFields][];
    for (int i = 0; i < numFields; i++) {
      fieldNames[i] = "f" + i;
      ArrayList<String> tokens = new ArrayList<String>();
      final String[] allTokens = loremIpsum.split("\\s");
      for (int index = random().nextInt(2 + i); index < allTokens.length; index += 1 + random()
          .nextInt(2 + i)) {
        tokens.add(allTokens[index].toLowerCase());
      }
      fieldTokens[i] = tokens.toArray(new String[tokens.size()]);
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }
  
  public void testEmptyIndex() throws IOException {
    // test performing fields addition and replace on an empty index
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    HashSet<Term> usedTerms = new HashSet<Term>();
    
    Operation operation = Operation.REPLACE_FIELDS;
    writer.updateFields(operation, getOperationTerm(usedTerms),
        getFields(usedTerms));
    
    operation = Operation.ADD_FIELDS;
    writer.updateFields(operation, getOperationTerm(usedTerms),
        getFields(usedTerms));
    
    writer.close();
    
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    assertEquals("Index should be empty", 0, directoryReader.maxDoc());
    directoryReader.close();
  }
  
  private void addDocuments() throws IOException {
    
    HashSet<Term> usedTerms = new HashSet<Term>();
    
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    // add random documents
    int numDocs = 10 + random().nextInt(50);
    int nCommits = 0;
    for (int i = 0; i < numDocs; i++) {
      
      // create fields
      Document fields = getFields(usedTerms);
      
      // select operation
      int opIndex = random().nextInt(10);
      Operation operation;
      if (opIndex <= 1) {
        if (opIndex == 0) {
          operation = Operation.REPLACE_FIELDS;
        } else {
          operation = Operation.ADD_FIELDS;
        }
        if (VERBOSE_FIELD_REPLACEMENTS) {
          System.out.println(operation);
        }
        
        // create term if needed
        Term term = getOperationTerm(usedTerms);
        
        writer.updateFields(operation, term, fields);
      } else {
        if (opIndex == 2) {
          if (VERBOSE_FIELD_REPLACEMENTS) {
            System.out.println("REPLACE_DOCUMENTS");
          }
          Term term = getOperationTerm(usedTerms);
          writer.replaceDocument(term, fields);
        } else {
          if (VERBOSE_FIELD_REPLACEMENTS) {
            System.out.println("ADD_DOCUMENT");
          }
          writer.addDocument(fields);
        }
      }
      
      // commit about once every 10 docs
      int interCommit = random().nextInt(10);
      if (interCommit == 0) {
        if (VERBOSE_FIELD_REPLACEMENTS) {
          System.out.println("commit " + (++nCommits));
        }
        writer.commit();
      }
    }
    if (VERBOSE_FIELD_REPLACEMENTS) {
      System.out.println("close");
    }
    writer.close();
  }
  
  public Document getFields(HashSet<Term> usedTerms) {
    Document fields = new Document();
    
    int nFields = 1 + random().nextInt(5);
    for (int j = 0; j < nFields; j++) {
      boolean indexed = random().nextInt(8) > 0;
      int index = random().nextInt(fieldNames.length);
      String fieldName = fieldNames[index];
      String value = createFieldValue(fieldTokens[index], fieldName, indexed,
          usedTerms);
      
      if (indexed) {
        fields.add(new TextField(fieldName, value, Store.NO));
        if (VERBOSE_FIELD_REPLACEMENTS) {
          System.out.print("Indexed\t");
        }
      } else {
        fields.add(new StoredField(fieldName, value));
        if (VERBOSE_FIELD_REPLACEMENTS) {
          System.out.print("Stored\t");
        }
      }
      if (VERBOSE_FIELD_REPLACEMENTS) {
        System.out.println(fieldName + "\t" + value);
      }
    }
    return fields;
  }
  
  public Term getOperationTerm(HashSet<Term> usedTerms) {
    Term term = null;
    boolean used = random().nextInt(5) < 4;
    if (used && !usedTerms.isEmpty()) {
      final Iterator<Term> iterator = usedTerms.iterator();
      int usedIndex = random().nextInt(usedTerms.size());
      for (int j = 0; j < usedIndex; j++) {
        iterator.next();
      }
      term = iterator.next();
    } else {
      // select term
      int fieldIndex = random().nextInt(fieldNames.length);
      int textIndex = random().nextInt(fieldTokens[fieldIndex].length / 10);
      term = new Term(fieldNames[fieldIndex],
          fieldTokens[fieldIndex][textIndex]);
    }
    if (VERBOSE_FIELD_REPLACEMENTS) {
      System.out.println("Term" + "\t" + term.field() + "\t" + term.text());
    }
    return term;
  }
  
  private String createFieldValue(String[] tokens, String fieldName,
      boolean indexed, HashSet<Term> usedTerms) {
    StringBuilder builder = new StringBuilder();
    
    int index = random().nextInt(Math.min(10, tokens.length));
    
    while (index < tokens.length) {
      builder.append(tokens[index]);
      builder.append(" ");
      if (indexed) {
        usedTerms.add(new Term(fieldName, tokens[index]));
      }
      index += 1 + random().nextInt(10);
    }
    
    return builder.toString();
  }
  
  public void testRandomIndexGeneration() throws IOException {
    addDocuments();
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    directoryReader.close();
  }
  
  public void testStatisticsAfterFieldUpdates() throws IOException {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FieldType fieldType = new FieldType();
    fieldType.setIndexed(true);
    fieldType.setTokenized(false);
    fieldType.setOmitNorms(true);
    fieldType.setStored(true);
    
    Document doc0 = new Document();
    doc0.add(new StoredField("f1", "a", fieldType));
    doc0.add(new StoredField("f1", "b", fieldType));
    writer.addDocument(doc0);
    
    Document doc1 = new Document();
    doc1.add(new StoredField("f1", "a", fieldType));
    doc1.add(new StoredField("f1", "c", fieldType));
    writer.addDocument(doc1);
    
    Document doc2 = new Document();
    doc2.add(new StoredField("f1", "b", fieldType));
    writer.addDocument(doc2);
    
    Document doc3 = new Document();
    doc3.add(new StoredField("f1", "d", fieldType));
    writer.updateFields(Operation.REPLACE_FIELDS, new Term("f1", "b"), doc3);
    
    writer.close();
    
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    final AtomicReader atomicReader = directoryReader.leaves().get(0).reader();
    printField(atomicReader, "f1");
    
    // check indexed fields
    final DocsAndPositionsEnum termPositionsA = atomicReader
        .termPositionsEnum(new Term("f1", "a"));
    assertEquals("wrong doc id", 1, termPositionsA.nextDoc());
    assertEquals("wrong position", 0, termPositionsA.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsA.nextDoc());
    
    final DocsAndPositionsEnum termPositionsB = atomicReader
        .termPositionsEnum(new Term("f1", "b"));
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsB.nextDoc());
    
    final DocsAndPositionsEnum termPositionsC = atomicReader
        .termPositionsEnum(new Term("f1", "c"));
    assertEquals("wrong doc id", 1, termPositionsC.nextDoc());
    assertEquals("wrong position", 1, termPositionsC.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsC.nextDoc());
    
    final DocsAndPositionsEnum termPositionsD = atomicReader
        .termPositionsEnum(new Term("f1", "d"));
    assertEquals("wrong doc id", 0, termPositionsD.nextDoc());
    assertEquals("wrong position", 0, termPositionsD.nextPosition());
    assertEquals("wrong doc id", 2, termPositionsD.nextDoc());
    assertEquals("wrong position", 0, termPositionsD.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsD.nextDoc());
    
    // check stored fields
    final StoredDocument stored0 = atomicReader.document(0);
    final StorableField[] f1_0 = stored0.getFields("f1");
    assertEquals("wrong numeber of stored fields", 1, f1_0.length);
    assertEquals("wrong field value", "d", f1_0[0].stringValue());

    final StoredDocument stored1 = atomicReader.document(1);
    final StorableField[] f1_1 = stored1.getFields("f1");
    assertEquals("wrong numeber of stored fields", 2, f1_1.length);
    assertEquals("wrong field value", "a", f1_1[0].stringValue());
    assertEquals("wrong field value", "c", f1_1[1].stringValue());
    
    final StoredDocument stored2 = atomicReader.document(2);
    final StorableField[] f1_2 = stored2.getFields("f1");
    assertEquals("wrong numeber of stored fields", 1, f1_2.length);
    assertEquals("wrong field value", "d", f1_2[0].stringValue());

    directoryReader.close();
    
  }
  
  private void printField(AtomicReader atomicReader, String fieldName)
      throws IOException {
    if (!VERBOSE_FIELD_REPLACEMENTS) {
      return;
    }
    
    System.out.println("field: " + fieldName);
    final Terms terms = atomicReader.fields().terms(fieldName);
    final TermsEnum iterator = terms.iterator(null);
    BytesRef term;
    while ((term = iterator.next()) != null) {
      System.out.println("term: " + term);
      final DocsAndPositionsEnum termPositionsEnum = atomicReader
          .termPositionsEnum(new Term(fieldName, term));
      while (termPositionsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        System.out.print("doc: " + termPositionsEnum.docID());
        for (int i = 0; i < termPositionsEnum.freq(); i++) {
          System.out.print("\t" + termPositionsEnum.nextPosition());
        }
        System.out.println();
      }
    }
  }
}
