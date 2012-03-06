package org.apache.lucene.index;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.InfoStream;

/**
 * Encapsulates all necessary state to initiate a {@link PerDocConsumer} and
 * create all necessary files in order to consume and merge per-document values.
 * 
 * @lucene.experimental
 */
public class PerDocWriteState {
  public final InfoStream infoStream;
  public final Directory directory;
  public final String segmentName;
  public final FieldInfos fieldInfos;
  public final Counter bytesUsed;
  public final String segmentSuffix;
  public final IOContext context;

  public PerDocWriteState(InfoStream infoStream, Directory directory,
      String segmentName, FieldInfos fieldInfos, Counter bytesUsed,
      String segmentSuffix, IOContext context) {
    this.infoStream = infoStream;
    this.directory = directory;
    this.segmentName = segmentName;
    this.fieldInfos = fieldInfos;
    this.segmentSuffix = segmentSuffix;
    this.bytesUsed = bytesUsed;
    this.context = context;
  }

  public PerDocWriteState(SegmentWriteState state) {
    infoStream = state.infoStream;
    directory = state.directory;
    segmentName = state.segmentName;
    fieldInfos = state.fieldInfos;
    segmentSuffix = state.segmentSuffix;
    bytesUsed = Counter.newCounter();
    context = state.context;
  }

  public PerDocWriteState(PerDocWriteState state, String segmentSuffix) {
    this.infoStream = state.infoStream;
    this.directory = state.directory;
    this.segmentName = state.segmentName;
    this.fieldInfos = state.fieldInfos;
    this.segmentSuffix = segmentSuffix;
    this.bytesUsed = state.bytesUsed;
    this.context = state.context;
  }
}
