package org.apache.lucene.index.store;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;

/**
 * Test to illustrate the problem found when trying to open an IndexWriter in
 * a situation where the the property <code>org.apache.lucene.lockDir</code>
 * was not set and the one specified by <code>java.io.tmpdir</code> had been
 * set to a non-existent path. What I observed is that this combination of
 * conditions resulted in a <code>NullPointerException</code> being thrown in
 * the <code>create()</code> method in <code>FSDirectory</code>, where
 * <code>files.length</code> is de-referenced, but <code>files</code> is
 * </code>null</code>.
 *
 * @author Michael Goddard
 */

public class FSDirectoryTest extends TestCase {

    /**
     * What happens if the Lucene lockDir doesn't exist?
     *
     * @throws Exception
     */
    public void testNonExistentTmpDir() throws Exception {
        orgApacheLuceneLockDir = System.setProperty(
                "org.apache.lucene.lockDir", NON_EXISTENT_DIRECTORY);
        String exceptionClassName = openIndexWriter();
        if (exceptionClassName == null
                || exceptionClassName.equals("java.io.IOException"))
            assertTrue(true);
        else
            fail("Caught an unexpected Exception");
    }

    /**
     * What happens if the Lucene lockDir is a regular file instead of a
     * directory?
     *
     * @throws Exception
     */
    public void testTmpDirIsPlainFile() throws Exception {
        shouldBeADirectory = new File(NON_EXISTENT_DIRECTORY);
        shouldBeADirectory.createNewFile();
        String exceptionClassName = openIndexWriter();
        if (exceptionClassName == null
                || exceptionClassName.equals("java.io.IOException"))
            assertTrue(true);
        else
            fail("Caught an unexpected Exception");
    }

    public static final String FILE_SEP = System.getProperty("file.separator");

    public static final String NON_EXISTENT_DIRECTORY = System
            .getProperty("java.io.tmpdir")
            + FILE_SEP + "highly_improbable_directory_name";

    public static final String TEST_INDEX_DIR = System
            .getProperty("java.io.tmpdir")
            + FILE_SEP + "temp_index";

    private String orgApacheLuceneLockDir;

    private File shouldBeADirectory;

    public void tearDown() {
        if (orgApacheLuceneLockDir != null)
            System.setProperty("org.apache.lucene.lockDir",
                    orgApacheLuceneLockDir);
        if (shouldBeADirectory != null && shouldBeADirectory.exists()) {
            try {
                shouldBeADirectory.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        File deletableIndex = new File(TEST_INDEX_DIR);
        if (deletableIndex.exists())
            try {
                rmDir(deletableIndex);
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

    /**
     * Open an IndexWriter<br>
     * Catch any (expected) IOException<br>
     * Close the IndexWriter
     */
    private static String openIndexWriter() {
        IndexWriter iw = null;
        String ret = null;
        try {
            iw = new IndexWriter(TEST_INDEX_DIR, new StandardAnalyzer(), true);
        } catch (IOException e) {
            ret = e.toString();
            e.printStackTrace();
        } catch (NullPointerException e) {
            ret = e.toString();
            e.printStackTrace();
        } finally {
            if (iw != null) {
                try {
                    iw.close();
                } catch (IOException ioe) {
                    // ignore this
                }
            }
        }
        return ret;
    }

    private static void rmDir(File dirName) throws Exception {
        if (dirName.exists()) {
            if (dirName.isDirectory()) {
                File[] contents = dirName.listFiles();
                for (int i = 0; i < contents.length; i++)
                    rmDir(contents[i]);
                dirName.delete();
            } else {
                dirName.delete();
            }
        }
	}
}
