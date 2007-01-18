package org.apache.lucene.store;

import java.util.HashSet;
import java.util.Random;
import java.io.File;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

public class TestWindowsMMap extends TestCase {
	
	private final static String alphabet = "abcdefghijklmnopqrstuvwzyz";
	private Random random;
	
	public void setUp() {
		random = new Random();
		System.setProperty("org.apache.lucene.FSDirectory.class", "org.apache.lucene.store.MMapDirectory");
	}
	
	private String randomToken() {
		int tl = 1 + random.nextInt(7);
		StringBuffer sb = new StringBuffer();
		for(int cx = 0; cx < tl; cx ++) {
			int c = random.nextInt(25);
			sb.append(alphabet.substring(c, c+1));
		}
		return sb.toString();
	}
	
	private String randomField() {
		int fl = 1 + random.nextInt(3);
		StringBuffer fb = new StringBuffer();
		for(int fx = 0; fx < fl; fx ++) {
			fb.append(randomToken());
			fb.append(" ");
		}
		return fb.toString();
	}
	
	private final static String storePathname = "testLuceneMmap";

	public void testMmapIndex() throws Exception {
		FSDirectory storeDirectory;
		storeDirectory = FSDirectory.getDirectory(storePathname);

		// plan to add a set of useful stopwords, consider changing some of the
		// interior filters.
		StandardAnalyzer analyzer = new StandardAnalyzer(new HashSet());
		// TODO: something about lock timeouts and leftover locks.
		IndexWriter writer = new IndexWriter(storeDirectory, analyzer, true);
		IndexSearcher searcher = new IndexSearcher(storePathname);
		
		for(int dx = 0; dx < 1000; dx ++) {
			String f = randomField();
			Document doc = new Document();
			doc.add(new Field("data", f, Field.Store.YES, Field.Index.TOKENIZED));	
			writer.addDocument(doc);
		}
		
		searcher.close();
		writer.close();
                rmDir(new File(storePathname));
	}

        private void rmDir(File dir) {
          File[] files = dir.listFiles();
          for (int i = 0; i < files.length; i++) {
            files[i].delete();
          }
          dir.delete();
        }
}
