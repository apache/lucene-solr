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

package org.apache.lucene.codecs.lucene80;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.composite.CompositeDocValuesProducer;
import org.apache.lucene.codecs.composite.CompositeFieldMetadata;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;

public class Lucene80SortedSetProducer implements CompositeDocValuesProducer.SortedProducer, CompositeDocValuesProducer.SortedSetProducer {
  private final int maxDoc;

  private final Map<String, SortedEntry> sorted = new HashMap<>();
  private final Map<String, SortedSetEntry> sortedSets = new HashMap<>();

  public Lucene80SortedSetProducer(int maxDoc) {
    this.maxDoc = maxDoc;
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field, CompositeFieldMetadata fieldMetadata, IndexInput indexInput) throws IOException {
    SortedEntry sortedEntry = getSortedEntry(field, fieldMetadata.getMetaStartFP(), indexInput);
    return getSorted(sortedEntry, indexInput);
  }

  private SortedEntry getSortedEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    SortedEntry sortedEntry = sorted.get(field.name);
    if (sortedEntry != null) {
      return sortedEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    sortedEntry = readSorted(clone);

    sorted.put(field.name, sortedEntry);
    return sortedEntry;
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException {
    SortedSetEntry sortedSetEntry = getSortedSetEntry(field, compositeFieldMetadata.getMetaStartFP(), indexInput);
    return getSortedSet(sortedSetEntry, indexInput);
  }

  private SortedSetEntry getSortedSetEntry(FieldInfo field, long metaStartFP, IndexInput indexInput) throws IOException {
    SortedSetEntry sortedSetEntry = sortedSets.get(field.name);
    if (sortedSetEntry != null) {
      return sortedSetEntry;
    }
    IndexInput clone = indexInput.clone();
    clone.seek(metaStartFP);
    sortedSetEntry = readSortedSet(clone);

    sortedSets.put(field.name, sortedSetEntry);
    return sortedSetEntry;
  }

  @Override
  public long ramBytesUsed() {
    long ramBytesUsed = 0L;
    for (Accountable accountable : sorted.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    for (Accountable accountable : sortedSets.values()) {
      ramBytesUsed += accountable.ramBytesUsed();
    }
    return ramBytesUsed;
  }

  public SortedSetDocValues getSortedSet(SortedSetEntry entry, IndexInput data) throws IOException {
    if (entry.singleValueEntry != null) {
      return DocValues.singleton(getSorted(entry.singleValueEntry, data));
    }

    final RandomAccessInput slice = data.randomAccessSlice(entry.ordsOffset, entry.ordsLength);
    final LongValues ords = DirectReader.getInstance(slice, entry.bitsPerValue);

    final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
    final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput);

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new BaseSortedSetDocValues(entry, data) {

        int doc = -1;
        long start;
        long end;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          doc = target;
          return true;
        }

        @Override
        public long nextOrd() throws IOException {
          if (start == end) {
            return NO_MORE_ORDS;
          }
          return ords.get(start++);
        }

      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new BaseSortedSetDocValues(entry, data) {

        boolean set;
        long start;
        long end = 0;

        @Override
        public int nextDoc() throws IOException {
          set = false;
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          set = false;
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public long nextOrd() throws IOException {
          if (set == false) {
            final int index = disi.index();
            final long start = addresses.get(index);
            this.start = start + 1;
            end = addresses.get(index + 1L);
            set = true;
            return ords.get(start);
          } else if (start == end) {
            return NO_MORE_ORDS;
          } else {
            return ords.get(start++);
          }
        }

      };
    }
  }

  static SortedSetEntry readSortedSet(IndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    byte multiValued = meta.readByte();
    switch (multiValued) {
      case 0: // singlevalued
        entry.singleValueEntry = readSorted(meta);
        return entry;
      case 1: // multivalued
        break;
      default:
        throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
    }
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.bitsPerValue = meta.readByte();
    entry.ordsOffset = meta.readLong();
    entry.ordsLength = meta.readLong();
    entry.numDocsWithField = meta.readInt();
    entry.addressesOffset = meta.readLong();
    final int blockShift = meta.readVInt();
    entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
    entry.addressesLength = meta.readLong();
    readTermDict(meta, entry);
    return entry;
  }

  private static void readTermDict(IndexInput meta, TermsDictEntry entry) throws IOException {
    entry.termsDictSize = meta.readVLong();
    entry.termsDictBlockShift = meta.readInt();
    final int blockShift = meta.readInt();
    final long addressesSize = (entry.termsDictSize + (1L << entry.termsDictBlockShift) - 1) >>> entry.termsDictBlockShift;
    entry.termsAddressesMeta = DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);
    entry.maxTermLength = meta.readInt();
    entry.termsDataOffset = meta.readLong();
    entry.termsDataLength = meta.readLong();
    entry.termsAddressesOffset = meta.readLong();
    entry.termsAddressesLength = meta.readLong();
    entry.termsDictIndexShift = meta.readInt();
    final long indexSize = (entry.termsDictSize + (1L << entry.termsDictIndexShift) - 1) >>> entry.termsDictIndexShift;
    entry.termsIndexAddressesMeta = DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);
    entry.termsIndexOffset = meta.readLong();
    entry.termsIndexLength = meta.readLong();
    entry.termsIndexAddressesOffset = meta.readLong();
    entry.termsIndexAddressesLength = meta.readLong();
  }

  private static class TermsDictEntry implements Accountable{
    long termsDictSize;
    int termsDictBlockShift;
    DirectMonotonicReader.Meta termsAddressesMeta;
    int maxTermLength;
    long termsDataOffset;
    long termsDataLength;
    long termsAddressesOffset;
    long termsAddressesLength;
    int termsDictIndexShift;
    DirectMonotonicReader.Meta termsIndexAddressesMeta;
    long termsIndexOffset;
    long termsIndexLength;
    long termsIndexAddressesOffset;
    long termsIndexAddressesLength;

    @Override
    public long ramBytesUsed() {
      return termsAddressesMeta.ramBytesUsed() + termsIndexAddressesMeta.ramBytesUsed();
    }
  }

  static class SortedEntry extends TermsDictEntry {
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    byte bitsPerValue;
    long ordsOffset;
    long ordsLength;
  }

  static class SortedSetEntry extends TermsDictEntry {
    SortedEntry singleValueEntry;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    byte bitsPerValue;
    long ordsOffset;
    long ordsLength;
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;

    @Override
    public long ramBytesUsed() {
      return addressesMeta == null ? 0L : addressesMeta.ramBytesUsed();
    }
  }

  static SortedEntry readSorted(IndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.bitsPerValue = meta.readByte();
    entry.ordsOffset = meta.readLong();
    entry.ordsLength = meta.readLong();
    readTermDict(meta, entry);
    return entry;
  }

  public SortedDocValues getSorted(SortedEntry entry, IndexInput data) throws IOException {
    if (entry.docsWithFieldOffset == -2) {
      return DocValues.emptySorted();
    }

    final LongValues ords;
    if (entry.bitsPerValue == 0) {
      ords = new LongValues() {
        @Override
        public long get(long index) {
          return 0L;
        }
      };
    } else {
      final RandomAccessInput slice = data.randomAccessSlice(entry.ordsOffset, entry.ordsLength);
      ords = DirectReader.getInstance(slice, entry.bitsPerValue);
    }

    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new BaseSortedDocValues(entry, data) {

        int doc = -1;

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public long cost() {
          return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) {
          doc = target;
          return true;
        }

        @Override
        public int ordValue() {
          return (int) ords.get(doc);
        }
      };
    } else {
      // sparse
      final IndexedDISI disi = new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength,
          entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);
      return new BaseSortedDocValues(entry, data) {

        @Override
        public int nextDoc() throws IOException {
          return disi.nextDoc();
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return disi.advanceExact(target);
        }

        @Override
        public int ordValue() {
          return (int) ords.get(disi.index());
        }
      };
    }
  }

  private static abstract class BaseSortedDocValues extends SortedDocValues {

    final SortedEntry entry;
    final IndexInput data;
    final TermsEnum termsEnum;

    BaseSortedDocValues(SortedEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      this.data = data;
      this.termsEnum = termsEnum();
    }

    @Override
    public int getValueCount() {
      return Math.toIntExact(entry.termsDictSize);
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public int lookupTerm(BytesRef key) throws IOException {
      TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return Math.toIntExact(termsEnum.ord());
        default:
          return Math.toIntExact(-1L - termsEnum.ord());
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data);
    }
  }

  private static abstract class BaseSortedSetDocValues extends SortedSetDocValues {

    final SortedSetEntry entry;
    final IndexInput data;
    final TermsEnum termsEnum;

    BaseSortedSetDocValues(SortedSetEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      this.data = data;
      this.termsEnum = termsEnum();
    }

    @Override
    public long getValueCount() {
      return entry.termsDictSize;
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public long lookupTerm(BytesRef key) throws IOException {
      TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return termsEnum.ord();
        default:
          return -1L - termsEnum.ord();
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data);
    }
  }

  private static class TermsDict extends BaseTermsEnum {

    final TermsDictEntry entry;
    final LongValues blockAddresses;
    final IndexInput bytes;
    final long blockMask;
    final LongValues indexAddresses;
    final IndexInput indexBytes;
    final BytesRef term;
    long ord = -1;

    TermsDict(TermsDictEntry entry, IndexInput data) throws IOException {
      this.entry = entry;
      RandomAccessInput addressesSlice = data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
      blockAddresses = DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice);
      bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
      blockMask = (1L << entry.termsDictBlockShift) - 1;
      RandomAccessInput indexAddressesSlice = data.randomAccessSlice(entry.termsIndexAddressesOffset, entry.termsIndexAddressesLength);
      indexAddresses = DirectMonotonicReader.getInstance(entry.termsIndexAddressesMeta, indexAddressesSlice);
      indexBytes = data.slice("terms-index", entry.termsIndexOffset, entry.termsIndexLength);
      term = new BytesRef(entry.maxTermLength);
    }

    @Override
    public BytesRef next() throws IOException {
      if (++ord >= entry.termsDictSize) {
        return null;
      }
      if ((ord & blockMask) == 0L) {
        term.length = bytes.readVInt();
        bytes.readBytes(term.bytes, 0, term.length);
      } else {
        final int token = Byte.toUnsignedInt(bytes.readByte());
        int prefixLength = token & 0x0F;
        int suffixLength = 1 + (token >>> 4);
        if (prefixLength == 15) {
          prefixLength += bytes.readVInt();
        }
        if (suffixLength == 16) {
          suffixLength += bytes.readVInt();
        }
        term.length = prefixLength + suffixLength;
        bytes.readBytes(term.bytes, prefixLength, suffixLength);
      }
      return term;
    }

    @Override
    public void seekExact(long ord) throws IOException {
      if (ord < 0 || ord >= entry.termsDictSize) {
        throw new IndexOutOfBoundsException();
      }
      final long blockIndex = ord >>> entry.termsDictBlockShift;
      final long blockAddress = blockAddresses.get(blockIndex);
      bytes.seek(blockAddress);
      this.ord = (blockIndex << entry.termsDictBlockShift) - 1;
      do {
        next();
      } while (this.ord < ord);
    }

    private BytesRef getTermFromIndex(long index) throws IOException {
      assert index >= 0 && index <= (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      final long start = indexAddresses.get(index);
      term.length = (int) (indexAddresses.get(index + 1) - start);
      indexBytes.seek(start);
      indexBytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekTermsIndex(BytesRef text) throws IOException {
      long lo = 0L;
      long hi = (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      while (lo <= hi) {
        final long mid = (lo + hi) >>> 1;
        getTermFromIndex(mid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }

      assert hi < 0 || getTermFromIndex(hi).compareTo(text) <= 0;
      assert hi == ((entry.termsDictSize - 1) >>> entry.termsDictIndexShift) || getTermFromIndex(hi + 1).compareTo(text) > 0;

      return hi;
    }

    private BytesRef getFirstTermFromBlock(long block) throws IOException {
      assert block >= 0 && block <= (entry.termsDictSize - 1) >>> entry.termsDictBlockShift;
      final long blockAddress = blockAddresses.get(block);
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekBlock(BytesRef text) throws IOException {
      long index = seekTermsIndex(text);
      if (index == -1L) {
        return -1L;
      }

      long ordLo = index << entry.termsDictIndexShift;
      long ordHi = Math.min(entry.termsDictSize, ordLo + (1L << entry.termsDictIndexShift)) - 1L;

      long blockLo = ordLo >>> entry.termsDictBlockShift;
      long blockHi = ordHi >>> entry.termsDictBlockShift;

      while (blockLo <= blockHi) {
        final long blockMid = (blockLo + blockHi) >>> 1;
        getFirstTermFromBlock(blockMid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          blockLo = blockMid + 1;
        } else {
          blockHi = blockMid - 1;
        }
      }

      assert blockHi < 0 || getFirstTermFromBlock(blockHi).compareTo(text) <= 0;
      assert blockHi == ((entry.termsDictSize - 1) >>> entry.termsDictBlockShift) || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

      return blockHi;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      final long block = seekBlock(text);
      if (block == -1) {
        // before the first term
        seekExact(0L);
        return SeekStatus.NOT_FOUND;
      }
      final long blockAddress = blockAddresses.get(block);
      this.ord = block << entry.termsDictBlockShift;
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      while (true) {
        int cmp = term.compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          return SeekStatus.NOT_FOUND;
        }
        if (next() == null) {
          return SeekStatus.END;
        }
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      return ord;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return -1L;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

}
