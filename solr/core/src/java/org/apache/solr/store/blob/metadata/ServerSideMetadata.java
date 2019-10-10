package org.apache.solr.store.blob.metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.solr.store.blob.client.BlobException;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;

/**
 * Object capturing the metadata of a shard index on a Solr node. 
 * 
 * This works in conjunction with {@link BlobCoreMetadata} to find the differences between 
 * local (Solr node) and remote (Blob store) commit point for a core.<p>
 * 
 * This object is somewhere between {@link org.apache.lucene.index.IndexCommit} and {@link org.apache.lucene.index.SegmentInfos}
 * and by implementing it separately we can add additional metadata to it as needed.
 */
public class ServerSideMetadata {
  
  /**
   * Files composing the core. They are are referenced from the core's current commit point's segments_N file
   * which is ALSO included in this collection.
   */
  private final ImmutableCollection<CoreFileData> latestCommitFiles;

  /**
   * Index files related to current and previous commit points(if any).
   * These files do not matter when pushing contents to blob but they do matter if blob content being pulled conflicts with them.
   */
  private final ImmutableCollection<CoreFileData> allCommitsFiles;

  /**
   * Hash of the directory content used to make sure the content doesn't change as we proceed to pull new files from Blob
   * (if we need to pull new files from Blob)
   */
  private final String directoryHash;

  /**
   * Generation number of the local index.
   * This generation number is only meant to identify a scenario where local index generation number is higher than
   * what we have in blob. In that scenario we would switch index to a new directory when pulling contents from blob. 
   * Because in the presence of higher generation number locally, blob contents cannot establish their legitimacy.
   */
  private final long generation;
  private final SolrCore core;
  private final String coreName;
  private final CoreContainer container;

  /**
   * Given a core name, builds the local metadata
   * 
   * 
   * @throws Exception if core corresponding to <code>coreName</code> can't be found.
   */
  public ServerSideMetadata(String coreName, CoreContainer container) throws Exception {
    this.coreName = coreName;
    this.container = container;
    this.core = container.getCore(coreName);

    if (core == null) {
      throw new Exception("Can't find core " + coreName);
    }

    try {
      IndexCommit latestCommit = core.getDeletionPolicy().getLatestCommit();
      if (latestCommit == null) {
        throw new BlobException("Core " + coreName + " has no available commit point");
      }

      generation = latestCommit.getGeneration();

      // Work around possible bug returning same file multiple times by using a set here
      // See org.apache.solr.handler.ReplicationHandler.getFileList()
      ImmutableCollection.Builder<CoreFileData> latestCommitBuilder = new ImmutableSet.Builder<>();
      ImmutableCollection.Builder<CoreFileData> allCommitsBuilder;

      Directory coreDir = core.getDirectoryFactory().get(core.getIndexDir(), DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
      try {
        // Capture now the hash and verify again if we need to pull content from the Blob store into this directory,
        // to make sure there are no local changes at the same time that might lead to a corruption in case of interaction
        // with the download.
        directoryHash = getSolrDirectoryHash(coreDir);

        buildCommitFiles(coreDir, latestCommit, latestCommitBuilder);

        // A note on listCommits says that it does not guarantee consistent results if a commit is in progress.
        // But in blob context we serialize commits and pulls by proper locking therefore we should be good here.
        List<IndexCommit> allCommits = DirectoryReader.listCommits(coreDir);

        // optimization:  normally we would only be dealing with one commit point. In that case just reuse latest commit files builder.
        if (allCommits.size() > 1 ){
          allCommitsBuilder = new ImmutableSet.Builder<>();
          for (IndexCommit commit: allCommits) {
            buildCommitFiles(coreDir, commit, allCommitsBuilder);
          }
        } else {
          // we should always have a commit point as verified in the beginning of this method.
          assert allCommits.size() == 1 && allCommits.get(0).equals(latestCommit);
          allCommitsBuilder = latestCommitBuilder;
        }
      } finally {
        core.getDirectoryFactory().release(coreDir);
      }
      latestCommitFiles = latestCommitBuilder.build();
      allCommitsFiles = allCommitsBuilder.build();
    } finally {
      core.close();
    }
  }

  private void buildCommitFiles(Directory coreDir, IndexCommit commit, ImmutableCollection.Builder<CoreFileData> builder) throws IOException {
    for (String fileName : commit.getFileNames()) {
      // Note we add here all segment related files as well as the commit point's segments_N file
      // Note commit points do not contain lock (write.lock) files.
      try (final IndexInput indexInput = coreDir.openInput(fileName, IOContext.READONCE)) {
        long length = indexInput.length();
        long checksum = CodecUtil.retrieveChecksum(indexInput);
        builder.add(new CoreFileData(fileName, length, checksum));
      }
    }
  }

  public String getCoreName() {
      return this.coreName;
  }

  public CoreContainer getCoreContainer() {
    return this.container;
  }

  public long getGeneration() {
    return this.generation;
  }

  public String getDirectoryHash() {
    return this.directoryHash;
  }

  public ImmutableCollection<CoreFileData> getLatestCommitFiles(){
    return this.latestCommitFiles;
  }

  public ImmutableCollection<CoreFileData> getAllCommitsFiles() {
    return this.allCommitsFiles;
  }

  /**
   * Returns <code>true</code> if the contents of the directory passed into this method is identical to the contents of
   * the directory of the Solr core of this instance, taken at instance creation time.<p>
   *
   * Passing in the Directory (expected to be the directory of the same core used during construction) because it seems
   * safer than trying to get it again here...
   */
  public boolean isSameDirectoryContent(Directory coreDir) throws NoSuchAlgorithmException, IOException {
    return directoryHash.equals(getSolrDirectoryHash(coreDir));
  }

  /**
   * Computes a hash of a Solr Directory in order to make sure the directory doesn't change as we pull content into it (if we need to
   * pull content into it)
   */
  private String getSolrDirectoryHash(Directory coreDir) throws NoSuchAlgorithmException, IOException {
    MessageDigest digest = MessageDigest.getInstance("sha1"); // not sure MD5 is available in Solr jars

    String[] filesNames = coreDir.listAll();
    // Computing the hash requires items to be submitted in the same order...
    Arrays.sort(filesNames);

    for (String fileName : filesNames) {
      // .lock files come and go. Ignore them (we're closing the Index Writer before adding any pulled files to the Core)
      if (!fileName.endsWith(".lock")) {
        // Hash the file name and file size so we can tell if any file has changed (or files appeared or vanished)
        digest.update(fileName.getBytes());
        try {
          digest.update(Long.toString(coreDir.fileLength(fileName)).getBytes());
        } catch (FileNotFoundException fnf) {
          // The file was deleted between the listAll() and the check, use an impossible size to not match a digest
          // for which the file is completely present or completely absent.
          digest.update(Long.toString(-42).getBytes());
        }
      }
    }

    final String hash = new String(Hex.encodeHex(digest.digest()));
    return hash;
  }

  @Override
  public String toString() {
    return "collectionName=" + core.getCoreDescriptor().getCollectionName() +
      " shardName=" + core.getCoreDescriptor().getCloudDescriptor().getShardId() +
      " coreName=" + core.getName() +
      " generation=" + generation;
  }

  /**
   * Information we capture per local core file (segments_N file *included*)
   */
  public static class CoreFileData {
    /** Local file name, no path */
    private final String fileName;
    /** Size in bytes */
    private final long fileSize;
    private final long checksum;

    CoreFileData(String fileName, long fileSize, long checksum) {
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.checksum = checksum;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
  
      CoreFileData other = (CoreFileData) o;
  
      return Objects.equals(fileName, other.fileName) &&
        Objects.equals(fileSize, other.fileSize) &&
        Objects.equals(checksum, other.checksum);
    }

    public String getFileName() {
      return fileName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getChecksum() {
      return checksum;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileName, fileSize, checksum);
    }
  }

}
