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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

/*
  Verify we can read previous versions' taxonomy indexes, do searches
  against them, and add documents to them.
*/
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows
// machines occasionally
public class TestBackwardsCompatibility extends LuceneTestCase {

  // To generate backcompat indexes with the current default codec, run the following gradle
  // command:
  //  gradlew test -Dtestcase=TestBackwardsCompatibility -Dtests.bwcdir=/path/to/store/indexes
  //           -Dtests.codec=default -Dtests.useSecurityManager=false
  // Also add testmethod with one of the index creation methods below, for example:
  //    -Dtestmethod=testCreateOldTaxonomy
  //
  // Zip up the generated indexes:
  //
  //    cd /path/to/store/indexes/index.cfs   ; zip index.<VERSION>-cfs.zip *
  //
  // Then move the zip file to your trunk checkout and use it in your test cases

  public static final String oldTaxonomyIndexName = "taxonomy.8.6.3-cfs";

  public void testCreateNewTaxonomy() throws IOException {
    createNewTaxonomyIndex(oldTaxonomyIndexName);
  }

  // Opens up a pre-existing old taxonomy index and adds new BinaryDocValues based fields
  private void createNewTaxonomyIndex(String dirName) throws IOException {
    Path indexDir = createTempDir(oldTaxonomyIndexName);
    TestUtil.unzip(getDataInputStream(dirName + ".zip"), indexDir);
    Directory dir = newFSDirectory(indexDir);

    DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);

    FacetLabel cp_b = new FacetLabel("b");
    writer.addCategory(cp_b);
    writer.getInternalIndexWriter().forceMerge(1);
    writer.commit();

    TaxonomyReader reader = new DirectoryTaxonomyReader(writer);

    int ord1 = reader.getOrdinal(new FacetLabel("a"));
    assert ord1 != TaxonomyReader.INVALID_ORDINAL;
    // Just asserting ord1 != TaxonomyReader.INVALID_ORDINAL is not enough to check compatibility
    assertNotNull(reader.getPath(ord1));

    int ord2 = reader.getOrdinal(cp_b);
    assert ord2 != TaxonomyReader.INVALID_ORDINAL;
    assertNotNull(reader.getPath(ord2));

    reader.close();
    writer.close();
    dir.close();
  }

  // Used to create a fresh taxonomy index with StoredFields
  @Ignore
  public void testCreateOldTaxonomy() throws IOException {
    createOldTaxonomyIndex(oldTaxonomyIndexName);
  }

  private void createOldTaxonomyIndex(String dirName) throws IOException {
    Path indexDir = getIndexDir().resolve(dirName);
    Files.deleteIfExists(indexDir);
    Directory dir = newFSDirectory(indexDir);

    TaxonomyWriter writer = new DirectoryTaxonomyWriter(dir);

    writer.addCategory(new FacetLabel("a"));
    writer.commit();
    writer.close();
    dir.close();
  }

  private Path getIndexDir() {
    String path = System.getProperty("tests.bwcdir");
    assumeTrue(
        "backcompat creation tests must be run with -Dtests.bwcdir=/path/to/write/indexes",
        path != null);
    return Paths.get(path);
  }
}
