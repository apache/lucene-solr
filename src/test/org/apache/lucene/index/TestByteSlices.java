package org.apache.lucene.index;

/**
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

import java.util.Random;
import java.util.ArrayList;
import org.apache.lucene.util.LuceneTestCase;

public class TestByteSlices extends LuceneTestCase {

  private static class ByteBlockAllocator extends ByteBlockPool.Allocator {
    ArrayList freeByteBlocks = new ArrayList();
    
    /* Allocate another byte[] from the shared pool */
    synchronized byte[] getByteBlock(boolean trackAllocations) {
      final int size = freeByteBlocks.size();
      final byte[] b;
      if (0 == size)
        b = new byte[DocumentsWriter.BYTE_BLOCK_SIZE];
      else
        b = (byte[]) freeByteBlocks.remove(size-1);
      return b;
    }

    /* Return a byte[] to the pool */
    synchronized void recycleByteBlocks(byte[][] blocks, int start, int end) {
      for(int i=start;i<end;i++)
        freeByteBlocks.add(blocks[i]);
    }
  }

  public void testBasic() throws Throwable {
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockAllocator(), false);

    final int NUM_STREAM = 25;

    ByteSliceWriter writer = new ByteSliceWriter(pool);

    int[] starts = new int[NUM_STREAM];
    int[] uptos = new int[NUM_STREAM];
    int[] counters = new int[NUM_STREAM];

    Random r = new Random(1);

    ByteSliceReader reader = new ByteSliceReader();

    for(int ti=0;ti<100;ti++) {

      for(int stream=0;stream<NUM_STREAM;stream++) {
        starts[stream] = -1;
        counters[stream] = 0;
      }
      
      boolean debug = false;

      for(int iter=0;iter<10000;iter++) {
        int stream = r.nextInt(NUM_STREAM);
        if (debug)
          System.out.println("write stream=" + stream);

        if (starts[stream] == -1) {
          final int spot = pool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
          starts[stream] = uptos[stream] = spot + pool.byteOffset;
          if (debug)
            System.out.println("  init to " + starts[stream]);
        }

        writer.init(uptos[stream]);
        int numValue = r.nextInt(20);
        for(int j=0;j<numValue;j++) {
          if (debug)
            System.out.println("    write " + (counters[stream]+j));
          writer.writeVInt(counters[stream]+j);
          //writer.writeVInt(ti);
        }
        counters[stream] += numValue;
        uptos[stream] = writer.getAddress();
        if (debug)
          System.out.println("    addr now " + uptos[stream]);
      }
    
      for(int stream=0;stream<NUM_STREAM;stream++) {
        if (debug)
          System.out.println("  stream=" + stream + " count=" + counters[stream]);

        if (starts[stream] != uptos[stream]) {
          reader.init(pool, starts[stream], uptos[stream]);
          for(int j=0;j<counters[stream];j++) 
            assertEquals(j, reader.readVInt());
            //assertEquals(ti, reader.readVInt());
        }
      }

      pool.reset();
    }
  }
}
