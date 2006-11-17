package org.apache.lucene.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.lucene.index.IndexModifier;

/**
 * This tests the patch for issue #LUCENE-715 (IndexWriter does not
 * release its write lock when trying to open an index which does not yet
 * exist).
 *
 * @author mbogosian
 * @version $Id$
 */

public class TestIndexWriterLockRelease extends TestCase {
    private java.io.File __test_dir;

    public void setUp() throws IOException {
        if (this.__test_dir == null) {
            String tmp_dir = System.getProperty("java.io.tmpdir", "tmp");
            this.__test_dir = new File(tmp_dir, "testIndexWriter");

            if (this.__test_dir.exists()) {
                throw new IOException("test directory \"" + this.__test_dir.getPath() + "\" already exists (please remove by hand)");
            }

            if (!this.__test_dir.mkdirs()
                && !this.__test_dir.isDirectory()) {
                throw new IOException("unable to create test directory \"" + this.__test_dir.getPath() + "\"");
            }
        }
    }

    public void tearDown() throws IOException {
        if (this.__test_dir != null) {
            File[] files = this.__test_dir.listFiles();

            for (int i = 0;
                i < files.length;
                ++i) {
                if (!files[i].delete()) {
                    throw new IOException("unable to remove file in test directory \"" + this.__test_dir.getPath() + "\" (please remove by hand)");
                }
            }

            if (!this.__test_dir.delete()) {
                throw new IOException("unable to remove test directory \"" + this.__test_dir.getPath() + "\" (please remove by hand)");
            }
        }
    }

    public void testIndexWriterLockRelease() throws IOException {
        IndexModifier im;

        try {
            im = new IndexModifier(this.__test_dir, new org.apache.lucene.analysis.standard.StandardAnalyzer(), false);
        } catch (FileNotFoundException e) {
            try {
                im = new IndexModifier(this.__test_dir, new org.apache.lucene.analysis.standard.StandardAnalyzer(), false);
            } catch (FileNotFoundException e1) {
            }
        }
    }
}
