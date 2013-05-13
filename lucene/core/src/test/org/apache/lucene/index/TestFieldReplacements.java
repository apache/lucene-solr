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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldsUpdate.Operation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IndexData;
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
        tokens.add(allTokens[index].toLowerCase(Locale.ROOT));
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
    init(random());
    
    // test performing fields addition and replacement on an empty index
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    HashSet<Term> usedTerms = new HashSet<Term>();
    
    Operation operation = Operation.REPLACE_FIELDS;
    writer.updateFields(operation, getOperationTerm(usedTerms, random()),
        getFields(usedTerms, random()));
    
    operation = Operation.ADD_FIELDS;
    writer.updateFields(operation, getOperationTerm(usedTerms, random()),
        getFields(usedTerms, random()));
    
    writer.close();
    
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    assertEquals("Index should be empty", 0, directoryReader.maxDoc());
    directoryReader.close();
  }
  
  private static void init(Random localRandom) {
    int numFields = 4 + localRandom.nextInt(4);
    fieldNames = new String[numFields];
    fieldTokens = new String[numFields][];
    for (int i = 0; i < numFields; i++) {
      fieldNames[i] = "f" + i;
      ArrayList<String> tokens = new ArrayList<String>();
      final String[] allTokens = loremIpsum.split("\\s");
      for (int index = localRandom.nextInt(2 + i); index < allTokens.length; index += 1 + localRandom
          .nextInt(2 + i)) {
        tokens.add(allTokens[index].toLowerCase());
      }
      fieldTokens[i] = tokens.toArray(new String[tokens.size()]);
    }
  }
  
  private static void addDocuments(Directory directory, Random localRandom,
      int maxDocs) throws IOException {
    init(localRandom);
    HashSet<Term> usedTerms = new HashSet<Term>();
    
    final IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    config.setCodec(new SimpleTextCodec());
    IndexWriter writer = new IndexWriter(directory, config);
    
    // add random documents
    int numOps = 10 + localRandom.nextInt(50);
    int nCommits = 0;
    for (int i = 0; i < Math.min(maxDocs, numOps); i++) {
      
      // select operation
      int opIndex = localRandom.nextInt(10);
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
        Term term = getOperationTerm(usedTerms, localRandom);
        
        // create fields and update
        writer.updateFields(operation, term, getFields(usedTerms, localRandom));
      } else {
        if (opIndex == 2) {
          if (VERBOSE_FIELD_REPLACEMENTS) {
            System.out.println("REPLACE_DOCUMENTS");
          }
          Term term = getOperationTerm(usedTerms, localRandom);
          // create document and replace
          writer.replaceDocument(term, getFields(usedTerms, localRandom));
        } else {
          if (VERBOSE_FIELD_REPLACEMENTS) {
            System.out.println("ADD_DOCUMENT");
          }
          // create document and add
          writer.addDocument(getFields(usedTerms, localRandom));
        }
      }
      
      // commit about once every 10 docs
      int interCommit = localRandom.nextInt(10);
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
  
  public static Document getFields(HashSet<Term> usedTerms, Random loaclRandom) {
    Document fields = new Document();
    
    int nFields = 1 + loaclRandom.nextInt(5);
    for (int j = 0; j < nFields; j++) {
      boolean indexed = loaclRandom.nextInt(8) > 0;
      int index = loaclRandom.nextInt(fieldNames.length);
      String fieldName = fieldNames[index];
      String value = createFieldValue(fieldTokens[index], fieldName, indexed,
          usedTerms, loaclRandom);
      
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
  
  public static Term getOperationTerm(HashSet<Term> usedTerms,
      Random loaclRandom) {
    Term term = null;
    boolean used = loaclRandom.nextInt(5) < 4;
    if (used && !usedTerms.isEmpty()) {
      final Iterator<Term> iterator = usedTerms.iterator();
      int usedIndex = loaclRandom.nextInt(usedTerms.size());
      for (int j = 0; j < usedIndex; j++) {
        iterator.next();
      }
      term = iterator.next();
    } else {
      // select term
      int fieldIndex = loaclRandom.nextInt(fieldNames.length);
      int textIndex = loaclRandom.nextInt(fieldTokens[fieldIndex].length / 10);
      term = new Term(fieldNames[fieldIndex],
          fieldTokens[fieldIndex][textIndex]);
    }
    if (VERBOSE_FIELD_REPLACEMENTS) {
      System.out.println("Term" + "\t" + term.field() + "\t" + term.text());
    }
    return term;
  }
  
  private static String createFieldValue(String[] tokens, String fieldName,
      boolean indexed, HashSet<Term> usedTerms, Random loaclRandom) {
    StringBuilder builder = new StringBuilder();
    
    int index = loaclRandom.nextInt(Math.min(10, tokens.length));
    
    while (index < tokens.length) {
      builder.append(tokens[index]);
      builder.append(" ");
      if (indexed) {
        usedTerms.add(new Term(fieldName, tokens[index]));
      }
      index += 1 + loaclRandom.nextInt(10);
    }
    
    return builder.toString();
  }
  
  public void testRandomIndexGeneration() throws IOException {
    addDocuments(dir, random(), Integer.MAX_VALUE);
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    directoryReader.close();
  }
  
  public void testAddIndexes() throws IOException {
    addDocuments(dir, random(), Integer.MAX_VALUE);
    RAMDirectory addedDir = new RAMDirectory();
    IndexWriter addedIndexWriter = new IndexWriter(addedDir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    addedIndexWriter.addIndexes(dir);
    addedIndexWriter.close();
    
    DirectoryReader updatesReader = DirectoryReader.open(dir);
    IndexData updatesIndexData = new IndexData(updatesReader);
    updatesReader.close();
    
    DirectoryReader addedReader = DirectoryReader.open(addedDir);
    IndexData addedIndexData = new IndexData(addedReader);
    addedReader.close();
    addedDir.close();
    
    assertEquals("Difference in addIndexes ", updatesIndexData, addedIndexData);
  }
  
  public void testIndexEquality() throws IOException {
    // create index through updates
    addDocuments(dir, new Random(3), Integer.MAX_VALUE);
    
    DirectoryReader updatesReader = DirectoryReader.open(dir);
    IndexData updatesIndexData = new IndexData(updatesReader);
    updatesReader.close();
    
    // create the same index directly
    RAMDirectory directDir = new RAMDirectory();
    IndexWriter directWriter = new IndexWriter(directDir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    Document doc = new Document();
    doc.add(new StoredField(
        "f0",
        "elit, magna volutpat. tation ea dolor consequat, facilisis odio te soluta doming facer qui me consuetudium littera per nunc "));
    doc.add(new TextField(
        "f4",
        "consectetuer tincidunt erat nostrud hendrerit dignissim claritatem me etiam quam claram, ",
        Store.NO));
    doc.add(new TextField(
        "f3",
        "nibh iriure qui liber claritatem. claram, seacula videntur sollemnes ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StoredField(
        "f0",
        "dolore quis duis iriure illum accumsan blandit tempor nihil facer assum. qui lectores dynamicus, claram, quinta qui sollemnes "));
    doc.add(new TextField(
        "f3",
        "wisi vel accumsan liber qui nunc qui ",
        Store.NO));
    doc.add(new TextField("f4",
        "adipiscing ea dolore claritatem. est litterarum qui fiant ", Store.NO));
    doc.add(new StoredField("f4",
        "diam hendrerit illum cum claritatem. quam claram, litterarum fiant "));
    doc.add(new TextField(
        "f1",
        "volutpat. nostrud lobortis dolore nulla odio blandit eleifend quod eorum qui formas nunc nobis ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f5",
        "magna dolore luptatum claritatem investigationes quod per ", Store.NO));
    doc.add(new TextField(
        "f2",
        "elit, sed dolore aliquip commodo eum dignissim feugait doming habent insitam; legunt est qui quarta parum ",
        Store.NO));
    doc.add(new StoredField(
        "f3",
        "nibh volutpat. in facilisis accumsan luptatum mazim lectores sequitur anteposuerit sollemnes "));
    doc.add(new TextField(
        "f2",
        "euismod suscipit eum dolor molestie at qui duis doming in lius qui notare nunc ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f4",
        "tincidunt velit facilisis dignissim cum iis claram, ", Store.NO));
    doc.add(new StoredField("f4",
        "ullamcorper accumsan delenit dolore nihil claritatem. mutationem clari, "));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f3",
        "exerci ea esse consequat, facilisis praesent placerat dynamicus, seacula qui ",
        Store.NO));
    doc.add(new TextField(
        "f2",
        "sed nonummy erat duis eum iriure dignissim duis nam assum. insitam; qui quam nunc futurum. ",
        Store.NO));
    doc.add(new TextField("f5", "velit luptatum augue placerat quam ", Store.NO));
    doc.add(new TextField("f3",
        "minim commodo facilisis qui imperdiet ii claritas seacula ", Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f2",
        "tincidunt suscipit dolor eu dignissim delenit congue possim lius anteposuerit in ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f5", "consectetuer illum eleifend processus fiant ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f1",
        "volutpat. minim aliquip duis dolore zzril congue in saepius. dynamicus, qui est eodem qui futurum. ",
        Store.NO));
    doc.add(new TextField(
        "f0",
        "ut quis duis eum hendrerit dolore odio feugait option doming mazim possim usus claritatem. legunt mirum litterarum qui sollemnes futurum. ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f0",
        "nibh ut ut minim exerci ea duis esse et blandit luptatum facilisi. soluta doming quod typi usus quod dynamicus, consuetudium mirum quam quarta clari, in ",
        Store.NO));
    doc.add(new TextField("f4",
        "wisi facilisis claritatem iis lius mutationem qui ", Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StoredField("f4",
        "nibh ullamcorper ea dignissim usus mutationem quarta "));
    doc.add(new StoredField("f4",
        "consectetuer wisi ea illum facilisis assum. mutationem quarta clari, "));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f2",
        "ipsum ad duis dolor eu at nam doming habent eorum me consuetudium decima futurum. ",
        Store.NO));
    doc.add(new StoredField("f5", "velit tempor processus putamus et typi, "));
    doc.add(new TextField(
        "f4",
        "adipiscing nibh wisi velit nulla nihil claritatem etiam quarta fiant ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f3",
        "adipiscing wisi nisl consequat, dignissim nobis qui mirum fiant sollemnes ",
        Store.NO));
    doc.add(new TextField(
        "f0",
        "euismod ut ad nisl dolor eu blandit te eleifend nihil typi qui lectores claritas consuetudium gothica, claram, decima sollemnes ",
        Store.NO));
    doc.add(new TextField(
        "f0",
        "ut erat ut nisl ea dolor velit vel eros odio qui feugait facilisi. nihil assum. usus ii legunt littera decima nobis sollemnes ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StoredField("f4",
        "nostrud velit accumsan quod assum. claritatem. etiam et in "));
    doc.add(new TextField("f4",
        "adipiscing ea facilisis nihil usus lius etiam qui ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f2",
        "dolore tation duis in eu delenit nam placerat in qui quarta ",
        Store.NO));
    doc.add(new TextField(
        "f2",
        "ut suscipit duis at dignissim delenit soluta insitam; me quam qui futurum. ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f0",
        "diam euismod quis autem consequat, eros iusto delenit feugait option quod habent claritatem claritatem. lectores consuetudium nunc per qui sollemnes ",
        Store.NO));
    doc.add(new TextField("f4",
        "nibh tincidunt hendrerit nulla usus est quam qui ", Store.NO));
    doc.add(new TextField(
        "f1",
        "adipiscing diam nostrud duis at zzril te nobis congue est demonstraverunt lius consuetudium est claram, qui in ",
        Store.NO));
    doc.add(new StoredField(
        "f0",
        "nibh euismod magna erat suscipit duis dolor esse et delenit tempor quod typi in legunt littera nunc decima. in "));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f4",
        "diam ullamcorper dignissim assum. claritatem. me etiam qui clari, ",
        Store.NO));
    doc.add(new TextField(
        "f0",
        "ipsum ut volutpat. minim autem dolor vulputate vel dolore odio blandit cum nobis mazim placerat facer possim est lectores sequitur consuetudium claram, modo qui in ",
        Store.NO));
    doc.add(new TextField(
        "f2",
        "tincidunt nisl duis in zzril placerat habent qui parum litterarum qui ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StoredField(
        "f3",
        "nibh ea consequat, accumsan tempor est dynamicus, seacula typi, videntur sollemnes "));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f1",
        "volutpat. duis dolor esse at iusto delenit doming est facit est consuetudium humanitatis sollemnes ",
        Store.NO));
    doc.add(new TextField(
        "f2",
        "ipsum ut nisl dolor dignissim nam placerat investigationes processus notare nunc in ",
        Store.NO));
    doc.add(new TextField(
        "f1",
        "ipsum tincidunt nostrud lobortis in vel nulla dolore placerat facit ii quam littera formas nunc clari, ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f3",
        "adipiscing ea consequat, qui nobis ii mirum et sollemnes ", Store.NO));
    doc.add(new TextField("f5", "velit te legere typi, ", Store.NO));
    doc.add(new TextField(
        "f3",
        "nisl in dignissim delenit placerat est claritatem. notare anteposuerit et videntur ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f1",
        "dolore nostrud suscipit lobortis duis vel et delenit liber cum habent usus claritatem. qui formas nobis sollemnes futurum. ",
        Store.NO));
    doc.add(new TextField(
        "f2",
        "erat tation duis in molestie dignissim liber congue possim me qui litterarum eodem in ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f0",
        "amet, elit, ut minim duis eum esse vel eu iusto blandit nam eleifend nihil typi usus facit legunt notare litterarum per decima clari, ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f2",
        "euismod suscipit in velit delenit facer legunt quam formas parum ",
        Store.NO));
    doc.add(new StoredField("f4",
        "ullamcorper accumsan delenit insitam; lius mutationem quarta decima. "));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StoredField(
        "f3",
        "ipsum adipiscing wisi nisl consequat, praesent placerat qui saepius. dynamicus, seacula videntur "));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new StoredField(
        "f0",
        "consectetuer erat minim suscipit ea esse consequat, feugiat accumsan duis cum nihil typi claritatem facit etiam quam claram, quinta modo futurum. "));
    doc.add(new TextField("f4",
        "tincidunt illum cum claritatem. mutationem litterarum ", Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField(
        "f2",
        "tincidunt erat ad aliquip duis velit dignissim delenit facer insitam; processus qui litterarum formas quarta ",
        Store.NO));
    doc.add(new StoredField("f2",
        "erat ut nisl duis at feugait congue in lius anteposuerit nunc "));
    doc.add(new TextField(
        "f3",
        "adipiscing minim esse luptatum tempor imperdiet est saepius. seacula fiant ",
        Store.NO));
    doc.add(new TextField(
        "f0",
        "ipsum elit, magna suscipit dolor eu iusto feugait eleifend quod assum. non est investigationes claritas nunc seacula videntur ",
        Store.NO));
    doc.add(new TextField(
        "f4",
        "amet, nibh ullamcorper velit nulla dignissim quod insitam; lius decima. ",
        Store.NO));
    directWriter.addDocument(doc);
    
    doc = new Document();
    doc.add(new TextField("f5",
        "quis dolore eleifend investigationes mirum per eodem typi, ", Store.NO));
    directWriter.addDocument(doc);
    
    directWriter.close();
    DirectoryReader directReader = DirectoryReader.open(directDir);
    
    IndexData directIndexData = new IndexData(directReader);
    directReader.close();
    directDir.close();
    
    boolean equalsNoOrder = IndexData.equalsNoOrder(directIndexData,
        updatesIndexData);
    assertTrue("indexes differ", equalsNoOrder);
  }
  
  public void testReplaceAndAddAgain() throws IOException {
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
    
    Document fields1 = new Document();
    fields1.add(new StoredField("f1", "d", fieldType));
    writer.updateFields(Operation.REPLACE_FIELDS, new Term("f1", "b"), fields1);
    
    Document doc2 = new Document();
    doc2.add(new StoredField("f1", "b", fieldType));
    writer.addDocument(doc2);
    
    writer.close();
    
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    final AtomicReader atomicReader = directoryReader.leaves().get(0).reader();
    printField(atomicReader, "f1");
    
    // check indexed fields
    final DocsAndPositionsEnum termPositionsA = atomicReader
        .termPositionsEnum(new Term("f1", "a"));
    assertNotNull("no positions for term", termPositionsA);
    assertEquals("wrong doc id", 1, termPositionsA.nextDoc());
    assertEquals("wrong position", 0, termPositionsA.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsA.nextDoc());
    
    final DocsAndPositionsEnum termPositionsB = atomicReader
        .termPositionsEnum(new Term("f1", "b"));
    assertNotNull("no positions for term", termPositionsB);
    assertEquals("wrong doc id", 2, termPositionsB.nextDoc());
    assertEquals("wrong position", 0, termPositionsB.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsB.nextDoc());
    
    final DocsAndPositionsEnum termPositionsC = atomicReader
        .termPositionsEnum(new Term("f1", "c"));
    assertNotNull("no positions for term", termPositionsC);
    assertEquals("wrong doc id", 1, termPositionsC.nextDoc());
    assertEquals("wrong position", 1, termPositionsC.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsC.nextDoc());
    
    final DocsAndPositionsEnum termPositionsD = atomicReader
        .termPositionsEnum(new Term("f1", "d"));
    assertNotNull("no positions for term", termPositionsD);
    assertEquals("wrong doc id", 0, termPositionsD.nextDoc());
    // 50000 == StackedDocsEnum.STACKED_SEGMENT_POSITION_INCREMENT
    assertEquals("wrong position", 50000, termPositionsD.nextPosition());
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
    assertEquals("wrong field value", "b", f1_2[0].stringValue());
    
    directoryReader.close();
    
  }
  
  public void testReplaceAndAddSameField() throws IOException {
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
    
    writer.commit();
    
    Document fields1 = new Document();
    fields1.add(new StoredField("f1", "d", fieldType));
    writer.updateFields(Operation.ADD_FIELDS, new Term("f1", "c"), fields1);
    
    writer.close();
    
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    final AtomicReader atomicReader = directoryReader.leaves().get(0).reader();
    printField(atomicReader, "f1");
    
    // check indexed fields
    final DocsAndPositionsEnum termPositionsA = atomicReader
        .termPositionsEnum(new Term("f1", "a"));
    assertNotNull("no positions for term", termPositionsA);
    assertEquals("wrong doc id", 0, termPositionsA.nextDoc());
    assertEquals("wrong position", 0, termPositionsA.nextPosition());
    assertEquals("wrong doc id", 1, termPositionsA.nextDoc());
    assertEquals("wrong position", 0, termPositionsA.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsA.nextDoc());
    
    final DocsAndPositionsEnum termPositionsB = atomicReader
        .termPositionsEnum(new Term("f1", "b"));
    assertNotNull("no positions for term", termPositionsB);
    assertEquals("wrong doc id", 0, termPositionsB.nextDoc());
    assertEquals("wrong position", 1, termPositionsB.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsB.nextDoc());
    
    final DocsAndPositionsEnum termPositionsC = atomicReader
        .termPositionsEnum(new Term("f1", "c"));
    assertNotNull("no positions for term", termPositionsC);
    assertEquals("wrong doc id", 1, termPositionsC.nextDoc());
    assertEquals("wrong position", 1, termPositionsC.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsC.nextDoc());
    
    final DocsAndPositionsEnum termPositionsD = atomicReader
        .termPositionsEnum(new Term("f1", "d"));
    assertNotNull("no positions for term", termPositionsD);
    assertEquals("wrong doc id", 1, termPositionsD.nextDoc());
    assertEquals("wrong position", 0, termPositionsD.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsD.nextDoc());
    
    // check stored fields
    final StoredDocument stored0 = atomicReader.document(0);
    final StorableField[] f1_0 = stored0.getFields("f1");
    assertEquals("wrong numeber of stored fields", 2, f1_0.length);
    assertEquals("wrong field value", "a", f1_0[0].stringValue());
    assertEquals("wrong field value", "b", f1_0[1].stringValue());
    
    final StoredDocument stored1 = atomicReader.document(1);
    final StorableField[] f1_1 = stored1.getFields("f1");
    assertEquals("wrong numeber of stored fields", 3, f1_1.length);
    assertEquals("wrong field value", "d", f1_1[0].stringValue());
    assertEquals("wrong field value", "a", f1_1[1].stringValue());
    assertEquals("wrong field value", "c", f1_1[2].stringValue());
    
    directoryReader.close();
    
  }
  
  public void testReplaceLayers() throws IOException {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FieldType fieldType = new FieldType();
    fieldType.setIndexed(true);
    fieldType.setTokenized(false);
    fieldType.setOmitNorms(true);
    fieldType.setStored(true);
    
    Document doc0 = new Document();
    doc0.add(new StoredField("f1", "a", fieldType));
    writer.addDocument(doc0);

    // add f2:b
    Document fields1 = new Document();
    fields1.add(new StoredField("f2", "b", fieldType));
    writer.updateFields(Operation.ADD_FIELDS, new Term("f1", "a"), fields1);
    
    // remove f2:b and add f2:c
    Document fields2 = new Document();
    fields2.add(new StoredField("f2", "c", fieldType));
    writer.updateFields(Operation.REPLACE_FIELDS, new Term("f2", "b"), fields2);
    
    // do nothing since f2:b was removed
    Document fields3 = new Document();
    fields3.add(new StoredField("f2", "d", fieldType));
    writer.updateFields(Operation.ADD_FIELDS, new Term("f2", "b"), fields3);
    
    writer.close();
    
    DirectoryReader directoryReader = DirectoryReader.open(dir);
    final AtomicReader atomicReader = directoryReader.leaves().get(0).reader();
    printField(atomicReader, "f1");
    
    // check indexed fields
    final DocsAndPositionsEnum termPositionsA = atomicReader
        .termPositionsEnum(new Term("f1", "a"));
    assertNotNull("no positions for term", termPositionsA);
    assertEquals("wrong doc id", 0, termPositionsA.nextDoc());
    assertEquals("wrong position", 0, termPositionsA.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsA.nextDoc());
    
    final DocsAndPositionsEnum termPositionsB = atomicReader
        .termPositionsEnum(new Term("f2", "b"));
    assertNotNull("no positions for term", termPositionsB);
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsB.nextDoc());
    
    final DocsAndPositionsEnum termPositionsC = atomicReader
        .termPositionsEnum(new Term("f2", "c"));
    assertNotNull("no positions for term", termPositionsC);
    assertEquals("wrong doc id", 0, termPositionsC.nextDoc());
    assertEquals("wrong position", 100000, termPositionsC.nextPosition());
    assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS,
        termPositionsC.nextDoc());
    
    final DocsAndPositionsEnum termPositionsD = atomicReader
        .termPositionsEnum(new Term("f2", "d"));
    assertNull("unexpected positions for term", termPositionsD);
    
    // check stored fields
    final StoredDocument stored0 = atomicReader.document(0);
    final StorableField[] f1_0 = stored0.getFields("f1");
    assertEquals("wrong numeber of stored fields", 1, f1_0.length);
    assertEquals("wrong field value", "a", f1_0[0].stringValue());
    final StorableField[] f2_0 = stored0.getFields("f2");
    assertEquals("wrong numeber of stored fields", 1, f2_0.length);
    assertEquals("wrong field value", "c", f2_0[0].stringValue());
    
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
  
  public void printIndexes() throws IOException {
    File outDir = new File("D:/temp/ifu/compare/scenario/b");
    outDir.mkdirs();
    
    for (int i = 0; i < 42; i++) {
      // Directory directory = new RAMDirectory();
      File fsDirFile = new File(outDir, "" + i);
      fsDirFile.mkdirs();
      Directory directory = FSDirectory.open(fsDirFile);
      for (String filename : directory.listAll()) {
        new File(fsDirFile, filename).delete();
      }
      addDocuments(directory, new Random(3), i);
      DirectoryReader updatesReader = DirectoryReader.open(directory);
      IndexData updatesIndexData = new IndexData(updatesReader);
      updatesReader.close();
      
      File out = new File(outDir, (i < 10 ? "0" : "") + i + ".txt");
      FileWriter fileWriter = new FileWriter(out);
      fileWriter.append(updatesIndexData.toString());
      fileWriter.close();
    }
  }

}
