package org.apache.lucene.store;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestBufferedIndexInput extends LuceneTestCase {
	// Call readByte() repeatedly, past the buffer boundary, and see that it
	// is working as expected.
	// Our input comes from a dynamically generated/ "file" - see
	// MyBufferedIndexInput below.
    public void testReadByte() throws Exception {
    	MyBufferedIndexInput input = new MyBufferedIndexInput(); 
    	for(int i=0; i<BufferedIndexInput.BUFFER_SIZE*10; i++){
     		assertEquals(input.readByte(), byten(i));
    	}
    }
 
	// Call readBytes() repeatedly, with various chunk sizes (from 1 byte to
    // larger than the buffer size), and see that it returns the bytes we expect.
	// Our input comes from a dynamically generated "file" -
    // see MyBufferedIndexInput below.
    public void testReadBytes() throws Exception {
    	MyBufferedIndexInput input = new MyBufferedIndexInput();
    	int pos=0;
    	// gradually increasing size:
    	for(int size=1; size<BufferedIndexInput.BUFFER_SIZE*10; size=size+size/200+1){
    		checkReadBytes(input, size, pos);
    		pos+=size;
    	}
    	// wildly fluctuating size:
    	for(long i=0; i<1000; i++){
    		// The following function generates a fluctuating (but repeatable)
    		// size, sometimes small (<100) but sometimes large (>10000)
    		int size1 = (int)( i%7 + 7*(i%5)+ 7*5*(i%3) + 5*5*3*(i%2));
    		int size2 = (int)( i%11 + 11*(i%7)+ 11*7*(i%5) + 11*7*5*(i%3) + 11*7*5*3*(i%2) );
    		int size = (i%3==0)?size2*10:size1; 
    		checkReadBytes(input, size, pos);
    		pos+=size;
    	}
    	// constant small size (7 bytes):
    	for(int i=0; i<BufferedIndexInput.BUFFER_SIZE; i++){
    		checkReadBytes(input, 7, pos);
    		pos+=7;
    	}
    }
   private void checkReadBytes(BufferedIndexInput input, int size, int pos) throws IOException{
	   // Just to see that "offset" is treated properly in readBytes(), we
	   // add an arbitrary offset at the beginning of the array
	   int offset = size % 10; // arbitrary
	   byte[] b = new byte[offset+size];
	   input.readBytes(b, offset, size);
	   for(int i=0; i<size; i++){
		   assertEquals(b[offset+i], byten(pos+i));
	   }
   }
   
   // This tests that attempts to readBytes() past an EOF will fail, while
   // reads up to the EOF will succeed. The EOF is determined by the
   // BufferedIndexInput's arbitrary length() value.
   public void testEOF() throws Exception {
	   MyBufferedIndexInput input = new MyBufferedIndexInput(1024);
	   // see that we can read all the bytes at one go:
	   checkReadBytes(input, (int)input.length(), 0);  
	   // go back and see that we can't read more than that, for small and
	   // large overflows:
	   int pos = (int)input.length()-10;
	   input.seek(pos);
	   checkReadBytes(input, 10, pos);  
	   input.seek(pos);
	   try {
		   checkReadBytes(input, 11, pos);
           fail("Block read past end of file");
       } catch (IOException e) {
           /* success */
       }
	   input.seek(pos);
	   try {
		   checkReadBytes(input, 50, pos);
           fail("Block read past end of file");
       } catch (IOException e) {
           /* success */
       }
	   input.seek(pos);
	   try {
		   checkReadBytes(input, 100000, pos);
           fail("Block read past end of file");
       } catch (IOException e) {
           /* success */
       }
  }

    // byten emulates a file - byten(n) returns the n'th byte in that file.
    // MyBufferedIndexInput reads this "file".
    private static byte byten(long n){
    	return (byte)(n*n%256);
    }
    private static class MyBufferedIndexInput extends BufferedIndexInput {
    	private long pos;
    	private long len;
    	public MyBufferedIndexInput(long len){
    		this.len = len;
    		this.pos = 0;
    	}
    	public MyBufferedIndexInput(){
    		// an infinite file
    		this(Long.MAX_VALUE);
    	}
		protected void readInternal(byte[] b, int offset, int length) throws IOException {
			for(int i=offset; i<offset+length; i++)
				b[i] = byten(pos++);
		}

		protected void seekInternal(long pos) throws IOException {
			this.pos = pos;
		}

		public void close() throws IOException {
		}

		public long length() {
			return len;
		}
    }

    public void testSetBufferSize() throws IOException {
      File indexDir = new File(System.getProperty("tempDir"), "testSetBufferSize");
      MockFSDirectory dir = new MockFSDirectory(indexDir);
      try {
        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        writer.setUseCompoundFile(false);
        for(int i=0;i<37;i++) {
          Document doc = new Document();
          doc.add(new Field("content", "aaa bbb ccc ddd" + i, Field.Store.YES, Field.Index.ANALYZED));
          doc.add(new Field("id", "" + i, Field.Store.YES, Field.Index.ANALYZED));
          writer.addDocument(doc);
        }
        writer.close();

        dir.allIndexInputs.clear();

        IndexReader reader = IndexReader.open(dir);
        Term aaa = new Term("content", "aaa");
        Term bbb = new Term("content", "bbb");
        Term ccc = new Term("content", "ccc");
        assertEquals(37, reader.docFreq(ccc));
        reader.deleteDocument(0);
        assertEquals(37, reader.docFreq(aaa));
        dir.tweakBufferSizes();
        reader.deleteDocument(4);
        assertEquals(reader.docFreq(bbb), 37);
        dir.tweakBufferSizes();

        IndexSearcher searcher = new IndexSearcher(reader);
        ScoreDoc[] hits = searcher.search(new TermQuery(bbb), null, 1000).scoreDocs;
        dir.tweakBufferSizes();
        assertEquals(35, hits.length);
        dir.tweakBufferSizes();
        hits = searcher.search(new TermQuery(new Term("id", "33")), null, 1000).scoreDocs;
        dir.tweakBufferSizes();
        assertEquals(1, hits.length);
        hits = searcher.search(new TermQuery(aaa), null, 1000).scoreDocs;
        dir.tweakBufferSizes();
        assertEquals(35, hits.length);
        searcher.close();
        reader.close();
      } finally {
        _TestUtil.rmDir(indexDir);
      }
    }

    private static class MockFSDirectory extends Directory {

      List allIndexInputs = new ArrayList();

      Random rand = new Random(788);

      private Directory dir;

      public MockFSDirectory(File path) throws IOException {
        lockFactory = new NoLockFactory();
        dir = FSDirectory.getDirectory(path);
      }

      public IndexInput openInput(String name) throws IOException {
        return openInput(name, BufferedIndexInput.BUFFER_SIZE);
      }

      public void tweakBufferSizes() {
        Iterator it = allIndexInputs.iterator();
        //int count = 0;
        while(it.hasNext()) {
          BufferedIndexInput bii = (BufferedIndexInput) it.next();
          int bufferSize = 1024+(int) Math.abs(rand.nextInt() % 32768);
          bii.setBufferSize(bufferSize);
          //count++;
        }
        //System.out.println("tweak'd " + count + " buffer sizes");
      }
      
      public IndexInput openInput(String name, int bufferSize) throws IOException {
        // Make random changes to buffer size
        bufferSize = 1+(int) Math.abs(rand.nextInt() % 10);
        IndexInput f = dir.openInput(name, bufferSize);
        allIndexInputs.add(f);
        return f;
      }

      public IndexOutput createOutput(String name) throws IOException {
        return dir.createOutput(name);
      }

      public void close() throws IOException {
        dir.close();
      }

      public void deleteFile(String name)
        throws IOException
      {
        dir.deleteFile(name);
      }
      public void touchFile(String name)
        throws IOException
      {
        dir.touchFile(name);
      }
      public long fileModified(String name)
        throws IOException
      {
        return dir.fileModified(name);
      }
      public boolean fileExists(String name)
        throws IOException
      {
        return dir.fileExists(name);
      }
      public String[] list()
        throws IOException
      {
        return dir.list();
      }

      public long fileLength(String name) throws IOException {
        return dir.fileLength(name);
      }
      public void renameFile(String from, String to)
        throws IOException
      {
        dir.renameFile(from, to);
      }


    }
}
