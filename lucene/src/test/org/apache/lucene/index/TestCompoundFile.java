package org.apache.lucene.index;

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

import java.io.IOException;
import java.io.File;

import org.apache.lucene.util.LuceneTestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store._TestHelper;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.util._TestUtil;


public class TestCompoundFile extends LuceneTestCase
{
    /** Main for running test case by itself. */
    public static void main(String args[]) {
        TestRunner.run (new TestSuite(TestCompoundFile.class));
//        TestRunner.run (new TestCompoundFile("testSingleFile"));
//        TestRunner.run (new TestCompoundFile("testTwoFiles"));
//        TestRunner.run (new TestCompoundFile("testRandomFiles"));
//        TestRunner.run (new TestCompoundFile("testClonedStreamsClosing"));
//        TestRunner.run (new TestCompoundFile("testReadAfterClose"));
//        TestRunner.run (new TestCompoundFile("testRandomAccess"));
//        TestRunner.run (new TestCompoundFile("testRandomAccessClones"));
//        TestRunner.run (new TestCompoundFile("testFileNotFound"));
//        TestRunner.run (new TestCompoundFile("testReadPastEOF"));

//        TestRunner.run (new TestCompoundFile("testIWCreate"));

    }


    private Directory dir;


    @Override
    public void setUp() throws Exception {
       super.setUp();
       File file = _TestUtil.getTempDir("testIndex");
       // use a simple FSDir here, to be sure to have SimpleFSInputs
       dir = new SimpleFSDirectory(file,null);
    }

    @Override
    public void tearDown() throws Exception {
       dir.close();
       super.tearDown();
    }

    /** Creates a file of the specified size with random data. */
    private void createRandomFile(Directory dir, String name, int size)
    throws IOException
    {
        IndexOutput os = dir.createOutput(name, newIOContext(random));
        for (int i=0; i<size; i++) {
            byte b = (byte) (Math.random() * 256);
            os.writeByte(b);
        }
        os.close();
    }

    /** Creates a file of the specified size with sequential data. The first
     *  byte is written as the start byte provided. All subsequent bytes are
     *  computed as start + offset where offset is the number of the byte.
     */
    private void createSequenceFile(Directory dir,
                                    String name,
                                    byte start,
                                    int size)
    throws IOException
    {
        IndexOutput os = dir.createOutput(name, newIOContext(random));
        for (int i=0; i < size; i++) {
            os.writeByte(start);
            start ++;
        }
        os.close();
    }


    private void assertSameStreams(String msg,
                                   IndexInput expected,
                                   IndexInput test)
    throws IOException
    {
        assertNotNull(msg + " null expected", expected);
        assertNotNull(msg + " null test", test);
        assertEquals(msg + " length", expected.length(), test.length());
        assertEquals(msg + " position", expected.getFilePointer(),
                                        test.getFilePointer());

        byte expectedBuffer[] = new byte[512];
        byte testBuffer[] = new byte[expectedBuffer.length];

        long remainder = expected.length() - expected.getFilePointer();
        while(remainder > 0) {
            int readLen = (int) Math.min(remainder, expectedBuffer.length);
            expected.readBytes(expectedBuffer, 0, readLen);
            test.readBytes(testBuffer, 0, readLen);
            assertEqualArrays(msg + ", remainder " + remainder, expectedBuffer,
                testBuffer, 0, readLen);
            remainder -= readLen;
        }
    }


    private void assertSameStreams(String msg,
                                   IndexInput expected,
                                   IndexInput actual,
                                   long seekTo)
    throws IOException
    {
        if(seekTo >= 0 && seekTo < expected.length())
        {
            expected.seek(seekTo);
            actual.seek(seekTo);
            assertSameStreams(msg + ", seek(mid)", expected, actual);
        }
    }



    private void assertSameSeekBehavior(String msg,
                                        IndexInput expected,
                                        IndexInput actual)
    throws IOException
    {
        // seek to 0
        long point = 0;
        assertSameStreams(msg + ", seek(0)", expected, actual, point);

        // seek to middle
        point = expected.length() / 2l;
        assertSameStreams(msg + ", seek(mid)", expected, actual, point);

        // seek to end - 2
        point = expected.length() - 2;
        assertSameStreams(msg + ", seek(end-2)", expected, actual, point);

        // seek to end - 1
        point = expected.length() - 1;
        assertSameStreams(msg + ", seek(end-1)", expected, actual, point);

        // seek to the end
        point = expected.length();
        assertSameStreams(msg + ", seek(end)", expected, actual, point);

        // seek past end
        point = expected.length() + 1;
        assertSameStreams(msg + ", seek(end+1)", expected, actual, point);
    }


    private void assertEqualArrays(String msg,
                                   byte[] expected,
                                   byte[] test,
                                   int start,
                                   int len)
    {
        assertNotNull(msg + " null expected", expected);
        assertNotNull(msg + " null test", test);

        for (int i=start; i<len; i++) {
            assertEquals(msg + " " + i, expected[i], test[i]);
        }
    }


    // ===========================================================
    //  Tests of the basic CompoundFile functionality
    // ===========================================================


    /** This test creates compound file based on a single file.
     *  Files of different sizes are tested: 0, 1, 10, 100 bytes.
     */
    public void testSingleFile() throws IOException {
        int data[] = new int[] { 0, 1, 10, 100 };
        for (int i=0; i<data.length; i++) {
            String name = "t" + data[i];
            createSequenceFile(dir, name, (byte) 0, data[i]);
            CompoundFileWriter csw = new CompoundFileWriter(dir, name + ".cfs", newIOContext(random));
            csw.addFile(name);
            csw.close();

            CompoundFileReader csr = new CompoundFileReader(dir, name + ".cfs", newIOContext(random));
            IndexInput expected = dir.openInput(name, newIOContext(random));
            IndexInput actual = csr.openInput(name, newIOContext(random));
            assertSameStreams(name, expected, actual);
            assertSameSeekBehavior(name, expected, actual);
            expected.close();
            actual.close();
            csr.close();
        }
    }


    /** This test creates compound file based on two files.
     *
     */
    public void testTwoFiles() throws IOException {
        createSequenceFile(dir, "d1", (byte) 0, 15);
        createSequenceFile(dir, "d2", (byte) 0, 114);

        CompoundFileWriter csw = new CompoundFileWriter(dir, "d.csf", newIOContext(random));
        csw.addFile("d1");
        csw.addFile("d2");
        csw.close();

        CompoundFileReader csr = new CompoundFileReader(dir, "d.csf", newIOContext(random));
        IndexInput expected = dir.openInput("d1", newIOContext(random));
        IndexInput actual = csr.openInput("d1", newIOContext(random));
        assertSameStreams("d1", expected, actual);
        assertSameSeekBehavior("d1", expected, actual);
        expected.close();
        actual.close();

        expected = dir.openInput("d2", newIOContext(random));
        actual = csr.openInput("d2", newIOContext(random));
        assertSameStreams("d2", expected, actual);
        assertSameSeekBehavior("d2", expected, actual);
        expected.close();
        actual.close();
        csr.close();
    }

    /** This test creates a compound file based on a large number of files of
     *  various length. The file content is generated randomly. The sizes range
     *  from 0 to 1Mb. Some of the sizes are selected to test the buffering
     *  logic in the file reading code. For this the chunk variable is set to
     *  the length of the buffer used internally by the compound file logic.
     */
    public void testRandomFiles() throws IOException {
        // Setup the test segment
        String segment = "test";
        int chunk = 1024; // internal buffer size used by the stream
        createRandomFile(dir, segment + ".zero", 0);
        createRandomFile(dir, segment + ".one", 1);
        createRandomFile(dir, segment + ".ten", 10);
        createRandomFile(dir, segment + ".hundred", 100);
        createRandomFile(dir, segment + ".big1", chunk);
        createRandomFile(dir, segment + ".big2", chunk - 1);
        createRandomFile(dir, segment + ".big3", chunk + 1);
        createRandomFile(dir, segment + ".big4", 3 * chunk);
        createRandomFile(dir, segment + ".big5", 3 * chunk - 1);
        createRandomFile(dir, segment + ".big6", 3 * chunk + 1);
        createRandomFile(dir, segment + ".big7", 1000 * chunk);

        // Setup extraneous files
        createRandomFile(dir, "onetwothree", 100);
        createRandomFile(dir, segment + ".notIn", 50);
        createRandomFile(dir, segment + ".notIn2", 51);

        // Now test
        CompoundFileWriter csw = new CompoundFileWriter(dir, "test.cfs", newIOContext(random));
        final String data[] = new String[] {
            ".zero", ".one", ".ten", ".hundred", ".big1", ".big2", ".big3",
            ".big4", ".big5", ".big6", ".big7"
        };
        for (int i=0; i<data.length; i++) {
            csw.addFile(segment + data[i]);
        }
        csw.close();

        CompoundFileReader csr = new CompoundFileReader(dir, "test.cfs", newIOContext(random));
        for (int i=0; i<data.length; i++) {
            IndexInput check = dir.openInput(segment + data[i], newIOContext(random));
            IndexInput test = csr.openInput(segment + data[i], newIOContext(random));
            assertSameStreams(data[i], check, test);
            assertSameSeekBehavior(data[i], check, test);
            test.close();
            check.close();
        }
        csr.close();
    }


    /** Setup a larger compound file with a number of components, each of
     *  which is a sequential file (so that we can easily tell that we are
     *  reading in the right byte). The methods sets up 20 files - f0 to f19,
     *  the size of each file is 1000 bytes.
     */
    private void setUp_2() throws IOException {
        CompoundFileWriter cw = new CompoundFileWriter(dir, "f.comp", newIOContext(random));
        for (int i=0; i<20; i++) {
            createSequenceFile(dir, "f" + i, (byte) 0, 2000);
            cw.addFile("f" + i);
        }
        cw.close();
    }


    public void testReadAfterClose() throws IOException {
        demo_FSIndexInputBug(dir, "test");
    }

    private void demo_FSIndexInputBug(Directory fsdir, String file)
    throws IOException
    {
        // IOContext triggers different buffer sizes so we use default here
        // Setup the test file - we need more than 1024 bytes
        IndexOutput os = fsdir.createOutput(file, IOContext.DEFAULT);
        for(int i=0; i<2000; i++) {
            os.writeByte((byte) i);
        }
        os.close();

        IndexInput in = fsdir.openInput(file, IOContext.DEFAULT);

        // This read primes the buffer in IndexInput
        in.readByte();

        // Close the file
        in.close();

        // ERROR: this call should fail, but succeeds because the buffer
        // is still filled
        in.readByte();

        // ERROR: this call should fail, but succeeds for some reason as well
        in.seek(1099);

        try {
            // OK: this call correctly fails. We are now past the 1024 internal
            // buffer, so an actual IO is attempted, which fails
            in.readByte();
            fail("expected readByte() to throw exception");
        } catch (IOException e) {
          // expected exception
        }
    }


    static boolean isCSIndexInput(IndexInput is) {
        return is instanceof CompoundFileReader.CSIndexInput;
    }

    static boolean isCSIndexInputOpen(IndexInput is) throws IOException {
        if (isCSIndexInput(is)) {
            CompoundFileReader.CSIndexInput cis =
            (CompoundFileReader.CSIndexInput) is;

            return _TestHelper.isSimpleFSIndexInputOpen(cis.base);
        } else {
            return false;
        }
    }


    public void testClonedStreamsClosing() throws IOException {
        setUp_2();
        CompoundFileReader cr = new CompoundFileReader(dir, "f.comp", newIOContext(random));

        // basic clone
        IndexInput expected = dir.openInput("f11", newIOContext(random));

        // this test only works for FSIndexInput
        assertTrue(_TestHelper.isSimpleFSIndexInput(expected));
        assertTrue(_TestHelper.isSimpleFSIndexInputOpen(expected));

        IndexInput one = cr.openInput("f11", newIOContext(random));
        assertTrue(isCSIndexInputOpen(one));

        IndexInput two = (IndexInput) one.clone();
        assertTrue(isCSIndexInputOpen(two));

        assertSameStreams("basic clone one", expected, one);
        expected.seek(0);
        assertSameStreams("basic clone two", expected, two);

        // Now close the first stream
        one.close();
        assertTrue("Only close when cr is closed", isCSIndexInputOpen(one));

        // The following should really fail since we couldn't expect to
        // access a file once close has been called on it (regardless of
        // buffering and/or clone magic)
        expected.seek(0);
        two.seek(0);
        assertSameStreams("basic clone two/2", expected, two);


        // Now close the compound reader
        cr.close();
        assertFalse("Now closed one", isCSIndexInputOpen(one));
        assertFalse("Now closed two", isCSIndexInputOpen(two));

        // The following may also fail since the compound stream is closed
        expected.seek(0);
        two.seek(0);
        //assertSameStreams("basic clone two/3", expected, two);


        // Now close the second clone
        two.close();
        expected.seek(0);
        two.seek(0);
        //assertSameStreams("basic clone two/4", expected, two);

        expected.close();
    }


    /** This test opens two files from a compound stream and verifies that
     *  their file positions are independent of each other.
     */
    public void testRandomAccess() throws IOException {
        setUp_2();
        CompoundFileReader cr = new CompoundFileReader(dir, "f.comp", newIOContext(random));

        // Open two files
        IndexInput e1 = dir.openInput("f11", newIOContext(random));
        IndexInput e2 = dir.openInput("f3", newIOContext(random));

        IndexInput a1 = cr.openInput("f11", newIOContext(random));
        IndexInput a2 = dir.openInput("f3", newIOContext(random));

        // Seek the first pair
        e1.seek(100);
        a1.seek(100);
        assertEquals(100, e1.getFilePointer());
        assertEquals(100, a1.getFilePointer());
        byte be1 = e1.readByte();
        byte ba1 = a1.readByte();
        assertEquals(be1, ba1);

        // Now seek the second pair
        e2.seek(1027);
        a2.seek(1027);
        assertEquals(1027, e2.getFilePointer());
        assertEquals(1027, a2.getFilePointer());
        byte be2 = e2.readByte();
        byte ba2 = a2.readByte();
        assertEquals(be2, ba2);

        // Now make sure the first one didn't move
        assertEquals(101, e1.getFilePointer());
        assertEquals(101, a1.getFilePointer());
        be1 = e1.readByte();
        ba1 = a1.readByte();
        assertEquals(be1, ba1);

        // Now more the first one again, past the buffer length
        e1.seek(1910);
        a1.seek(1910);
        assertEquals(1910, e1.getFilePointer());
        assertEquals(1910, a1.getFilePointer());
        be1 = e1.readByte();
        ba1 = a1.readByte();
        assertEquals(be1, ba1);

        // Now make sure the second set didn't move
        assertEquals(1028, e2.getFilePointer());
        assertEquals(1028, a2.getFilePointer());
        be2 = e2.readByte();
        ba2 = a2.readByte();
        assertEquals(be2, ba2);

        // Move the second set back, again cross the buffer size
        e2.seek(17);
        a2.seek(17);
        assertEquals(17, e2.getFilePointer());
        assertEquals(17, a2.getFilePointer());
        be2 = e2.readByte();
        ba2 = a2.readByte();
        assertEquals(be2, ba2);

        // Finally, make sure the first set didn't move
        // Now make sure the first one didn't move
        assertEquals(1911, e1.getFilePointer());
        assertEquals(1911, a1.getFilePointer());
        be1 = e1.readByte();
        ba1 = a1.readByte();
        assertEquals(be1, ba1);

        e1.close();
        e2.close();
        a1.close();
        a2.close();
        cr.close();
    }

    /** This test opens two files from a compound stream and verifies that
     *  their file positions are independent of each other.
     */
    public void testRandomAccessClones() throws IOException {
        setUp_2();
        CompoundFileReader cr = new CompoundFileReader(dir, "f.comp", newIOContext(random));

        // Open two files
        IndexInput e1 = cr.openInput("f11", newIOContext(random));
        IndexInput e2 = cr.openInput("f3", newIOContext(random));

        IndexInput a1 = (IndexInput) e1.clone();
        IndexInput a2 = (IndexInput) e2.clone();

        // Seek the first pair
        e1.seek(100);
        a1.seek(100);
        assertEquals(100, e1.getFilePointer());
        assertEquals(100, a1.getFilePointer());
        byte be1 = e1.readByte();
        byte ba1 = a1.readByte();
        assertEquals(be1, ba1);

        // Now seek the second pair
        e2.seek(1027);
        a2.seek(1027);
        assertEquals(1027, e2.getFilePointer());
        assertEquals(1027, a2.getFilePointer());
        byte be2 = e2.readByte();
        byte ba2 = a2.readByte();
        assertEquals(be2, ba2);

        // Now make sure the first one didn't move
        assertEquals(101, e1.getFilePointer());
        assertEquals(101, a1.getFilePointer());
        be1 = e1.readByte();
        ba1 = a1.readByte();
        assertEquals(be1, ba1);

        // Now more the first one again, past the buffer length
        e1.seek(1910);
        a1.seek(1910);
        assertEquals(1910, e1.getFilePointer());
        assertEquals(1910, a1.getFilePointer());
        be1 = e1.readByte();
        ba1 = a1.readByte();
        assertEquals(be1, ba1);

        // Now make sure the second set didn't move
        assertEquals(1028, e2.getFilePointer());
        assertEquals(1028, a2.getFilePointer());
        be2 = e2.readByte();
        ba2 = a2.readByte();
        assertEquals(be2, ba2);

        // Move the second set back, again cross the buffer size
        e2.seek(17);
        a2.seek(17);
        assertEquals(17, e2.getFilePointer());
        assertEquals(17, a2.getFilePointer());
        be2 = e2.readByte();
        ba2 = a2.readByte();
        assertEquals(be2, ba2);

        // Finally, make sure the first set didn't move
        // Now make sure the first one didn't move
        assertEquals(1911, e1.getFilePointer());
        assertEquals(1911, a1.getFilePointer());
        be1 = e1.readByte();
        ba1 = a1.readByte();
        assertEquals(be1, ba1);

        e1.close();
        e2.close();
        a1.close();
        a2.close();
        cr.close();
    }


    public void testFileNotFound() throws IOException {
        setUp_2();
        CompoundFileReader cr = new CompoundFileReader(dir, "f.comp", newIOContext(random));

        // Open two files
        try {
            cr.openInput("bogus", newIOContext(random));
            fail("File not found");

        } catch (IOException e) {
            /* success */
            //System.out.println("SUCCESS: File Not Found: " + e);
        }

        cr.close();
    }


    public void testReadPastEOF() throws IOException {
        setUp_2();
        CompoundFileReader cr = new CompoundFileReader(dir, "f.comp", newIOContext(random));
        IndexInput is = cr.openInput("f2", newIOContext(random));
        is.seek(is.length() - 10);
        byte b[] = new byte[100];
        is.readBytes(b, 0, 10);

        try {
            is.readByte();
            fail("Single byte read past end of file");
        } catch (IOException e) {
            /* success */
            //System.out.println("SUCCESS: single byte read past end of file: " + e);
        }

        is.seek(is.length() - 10);
        try {
            is.readBytes(b, 0, 50);
            fail("Block read past end of file");
        } catch (IOException e) {
            /* success */
            //System.out.println("SUCCESS: block read past end of file: " + e);
        }

        is.close();
        cr.close();
    }

    /** This test that writes larger than the size of the buffer output
     * will correctly increment the file pointer.
     */
    public void testLargeWrites() throws IOException {
        IndexOutput os = dir.createOutput("testBufferStart.txt", newIOContext(random));

        byte[] largeBuf = new byte[2048];
        for (int i=0; i<largeBuf.length; i++) {
            largeBuf[i] = (byte) (Math.random() * 256);
        }

        long currentPos = os.getFilePointer();
        os.writeBytes(largeBuf, largeBuf.length);

        try {
            assertEquals(currentPos + largeBuf.length, os.getFilePointer());
        } finally {
            os.close();
        }

    }
    
   public void testAddExternalFile() throws IOException {
       createSequenceFile(dir, "d1", (byte) 0, 15);

       Directory newDir = newDirectory();
       CompoundFileWriter csw = new CompoundFileWriter(newDir, "d.csf", newIOContext(random));
       csw.addFile("d1", dir);
       csw.close();

       CompoundFileReader csr = new CompoundFileReader(newDir, "d.csf", newIOContext(random));
       IndexInput expected = dir.openInput("d1", newIOContext(random));
       IndexInput actual = csr.openInput("d1", newIOContext(random));
       assertSameStreams("d1", expected, actual);
       assertSameSeekBehavior("d1", expected, actual);
       expected.close();
       actual.close();
       csr.close();
       
       newDir.close();
   }

}
