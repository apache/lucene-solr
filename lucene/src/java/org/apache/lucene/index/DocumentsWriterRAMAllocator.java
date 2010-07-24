package org.apache.lucene.index;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.Constants;

class DocumentsWriterRAMAllocator {
  final ByteBlockAllocator byteBlockAllocator = new ByteBlockAllocator(BYTE_BLOCK_SIZE);
  final ByteBlockAllocator perDocAllocator = new ByteBlockAllocator(PER_DOC_BLOCK_SIZE);

  
  class ByteBlockAllocator extends ByteBlockPool.Allocator {
    final int blockSize;

    ByteBlockAllocator(int blockSize) {
      this.blockSize = blockSize;
    }

    ArrayList<byte[]> freeByteBlocks = new ArrayList<byte[]>();
    
    /* Allocate another byte[] from the shared pool */
    @Override
    byte[] getByteBlock() {
      final int size = freeByteBlocks.size();
      final byte[] b;
      if (0 == size) {
        b = new byte[blockSize];
        // Always record a block allocated, even if
        // trackAllocations is false.  This is necessary
        // because this block will be shared between
        // things that don't track allocations (term
        // vectors) and things that do (freq/prox
        // postings).
        numBytesUsed += blockSize;
      } else
        b = freeByteBlocks.remove(size-1);
      return b;
    }

    /* Return byte[]'s to the pool */
    @Override
    void recycleByteBlocks(byte[][] blocks, int start, int end) {
      for(int i=start;i<end;i++) {
        freeByteBlocks.add(blocks[i]);
      }
    }

    @Override
    void recycleByteBlocks(List<byte[]> blocks) {
      final int size = blocks.size();
      for(int i=0;i<size;i++) {
        freeByteBlocks.add(blocks.get(i));
      }
    }
  }

  private ArrayList<int[]> freeIntBlocks = new ArrayList<int[]>();

  /* Allocate another int[] from the shared pool */
  int[] getIntBlock() {
    final int size = freeIntBlocks.size();
    final int[] b;
    if (0 == size) {
      b = new int[INT_BLOCK_SIZE];
      // Always record a block allocated, even if
      // trackAllocations is false.  This is necessary
      // because this block will be shared between
      // things that don't track allocations (term
      // vectors) and things that do (freq/prox
      // postings).
      numBytesUsed += INT_BLOCK_SIZE*INT_NUM_BYTE;
    } else
      b = freeIntBlocks.remove(size-1);
    return b;
  }

  void bytesUsed(long numBytes) {
    numBytesUsed += numBytes;
  }

  /* Return int[]s to the pool */
  void recycleIntBlocks(int[][] blocks, int start, int end) {
    for(int i=start;i<end;i++)
      freeIntBlocks.add(blocks[i]);
  }

  long getRAMUsed() {
    return numBytesUsed;
  }

  long numBytesUsed;

  NumberFormat nf = NumberFormat.getInstance();

  final static int PER_DOC_BLOCK_SIZE = 1024;
  
  // Coarse estimates used to measure RAM usage of buffered deletes
  final static int OBJECT_HEADER_BYTES = 8;
  final static int POINTER_NUM_BYTE = Constants.JRE_IS_64BIT ? 8 : 4;
  final static int INT_NUM_BYTE = 4;
  final static int CHAR_NUM_BYTE = 2;

  /* Rough logic: HashMap has an array[Entry] w/ varying
     load factor (say 2 * POINTER).  Entry is object w/ Term
     key, BufferedDeletes.Num val, int hash, Entry next
     (OBJ_HEADER + 3*POINTER + INT).  Term is object w/
     String field and String text (OBJ_HEADER + 2*POINTER).
     We don't count Term's field since it's interned.
     Term's text is String (OBJ_HEADER + 4*INT + POINTER +
     OBJ_HEADER + string.length*CHAR).  BufferedDeletes.num is
     OBJ_HEADER + INT. */
 
  final static int BYTES_PER_DEL_TERM = 8*POINTER_NUM_BYTE + 5*OBJECT_HEADER_BYTES + 6*INT_NUM_BYTE;

  /* Rough logic: del docIDs are List<Integer>.  Say list
     allocates ~2X size (2*POINTER).  Integer is OBJ_HEADER
     + int */
  final static int BYTES_PER_DEL_DOCID = 2*POINTER_NUM_BYTE + OBJECT_HEADER_BYTES + INT_NUM_BYTE;

  /* Rough logic: HashMap has an array[Entry] w/ varying
     load factor (say 2 * POINTER).  Entry is object w/
     Query key, Integer val, int hash, Entry next
     (OBJ_HEADER + 3*POINTER + INT).  Query we often
     undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
  final static int BYTES_PER_DEL_QUERY = 5*POINTER_NUM_BYTE + 2*OBJECT_HEADER_BYTES + 2*INT_NUM_BYTE + 24;

  /* Initial chunks size of the shared byte[] blocks used to
     store postings data */
  final static int BYTE_BLOCK_SHIFT = 15;
  final static int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
  final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;
  final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

  final static int MAX_TERM_LENGTH_UTF8 = BYTE_BLOCK_SIZE-2;

  /* Initial chunks size of the shared int[] blocks used to
     store postings data */
  final static int INT_BLOCK_SHIFT = 13;
  final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
  final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

  String toMB(long v) {
    return nf.format(v/1024./1024.);
  }
}
