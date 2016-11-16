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

package org.apache.solr.core.snapshots;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class provides utility functions required for Solr snapshots functionality.
 */
public class SolrSnapshotsTool implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final DateFormat dateFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.getDefault());

  private static final String CREATE = "create";
  private static final String DELETE = "delete";
  private static final String LIST = "list";
  private static final String DESCRIBE = "describe";
  private static final String PREPARE_FOR_EXPORT = "prepare-snapshot-export";
  private static final String EXPORT_SNAPSHOT = "export";
  private static final String HELP = "help";
  private static final String COLLECTION = "c";
  private static final String TEMP_DIR = "t";
  private static final String DEST_DIR = "d";
  private static final String SOLR_ZK_ENSEMBLE = "z";
  private static final String HDFS_PATH_PREFIX = "p";
  private static final String BACKUP_REPO_NAME = "r";
  private static final String ASYNC_REQ_ID = "i";
  private static final List<String> OPTION_HELP_ORDER = Arrays.asList(CREATE, DELETE, LIST, DESCRIBE,
      PREPARE_FOR_EXPORT, EXPORT_SNAPSHOT, HELP, SOLR_ZK_ENSEMBLE, COLLECTION, DEST_DIR, BACKUP_REPO_NAME,
      ASYNC_REQ_ID, TEMP_DIR, HDFS_PATH_PREFIX);

  private final CloudSolrClient solrClient;

  public SolrSnapshotsTool(String solrZkEnsemble) {
    solrClient = (new CloudSolrClient.Builder()).withZkHost(solrZkEnsemble).build();
  }

  @Override
  public void close() throws IOException {
    if (solrClient != null) {
      solrClient.close();
    }
  }

  public void createSnapshot(String collectionName, String snapshotName) {
    CollectionAdminRequest.CreateSnapshot createSnap = new CollectionAdminRequest.CreateSnapshot(collectionName, snapshotName);
    CollectionAdminResponse resp;
    try {
      resp = createSnap.process(solrClient);
      Preconditions.checkState(resp.getStatus() == 0, "The CREATESNAPSHOT request failed. The status code is " + resp.getStatus());
      System.out.println("Successfully created snapshot with name " + snapshotName + " for collection " + collectionName);

    } catch (Exception e) {
      log.error("Failed to create a snapshot with name " + snapshotName + " for collection " + collectionName, e);
      System.out.println("Failed to create a snapshot with name " + snapshotName + " for collection " + collectionName
          +" due to following error : "+e.getLocalizedMessage());
    }
  }

  public void deleteSnapshot(String collectionName, String snapshotName) {
    CollectionAdminRequest.DeleteSnapshot deleteSnap = new CollectionAdminRequest.DeleteSnapshot(collectionName, snapshotName);
    CollectionAdminResponse resp;
    try {
      resp = deleteSnap.process(solrClient);
      Preconditions.checkState(resp.getStatus() == 0, "The DELETESNAPSHOT request failed. The status code is " + resp.getStatus());
      System.out.println("Successfully deleted snapshot with name " + snapshotName + " for collection " + collectionName);

    } catch (Exception e) {
      log.error("Failed to delete a snapshot with name " + snapshotName + " for collection " + collectionName, e);
      System.out.println("Failed to delete a snapshot with name " + snapshotName + " for collection " + collectionName
          +" due to following error : "+e.getLocalizedMessage());
    }
  }

  @SuppressWarnings("rawtypes")
  public void listSnapshots(String collectionName) {
    CollectionAdminRequest.ListSnapshots listSnaps = new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp;
    try {
      resp = listSnaps.process(solrClient);
      Preconditions.checkState(resp.getStatus() == 0, "The LISTSNAPSHOTS request failed. The status code is " + resp.getStatus());

      NamedList apiResult = (NamedList) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);
      for (int i = 0; i < apiResult.size(); i++) {
        System.out.println(apiResult.getName(i));
      }

    } catch (Exception e) {
      log.error("Failed to list snapshots for collection " + collectionName, e);
      System.out.println("Failed to list snapshots for collection " + collectionName
          +" due to following error : "+e.getLocalizedMessage());
    }
  }

  public void describeSnapshot(String collectionName, String snapshotName) {
    try {
      Collection<CollectionSnapshotMetaData> snaps = listCollectionSnapshots(collectionName);
      for (CollectionSnapshotMetaData m : snaps) {
        if (snapshotName.equals(m.getName())) {
          System.out.println("Name: " + m.getName());
          System.out.println("Status: " + m.getStatus());
          System.out.println("Time of creation: " + dateFormat.format(m.getCreationDate()));
          System.out.println("Total number of cores with snapshot: " + m.getReplicaSnapshots().size());
          System.out.println("-----------------------------------");
          for (CoreSnapshotMetaData n : m.getReplicaSnapshots()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Core [name=");
            builder.append(n.getCoreName());
            builder.append(", leader=");
            builder.append(n.isLeader());
            builder.append(", generation=");
            builder.append(n.getGenerationNumber());
            builder.append(", indexDirPath=");
            builder.append(n.getIndexDirPath());
            builder.append("]\n");
            System.out.println(builder.toString());
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed to fetch snapshot details", e);
      System.out.println("Failed to fetch snapshot details due to following error : " + e.getLocalizedMessage());
    }
  }

  public Map<String, List<String>> getIndexFilesPathForSnapshot(String collectionName,  String snapshotName, Optional<String> pathPrefix)
      throws SolrServerException, IOException {
    Map<String, List<String>> result = new HashMap<>();

    Collection<CollectionSnapshotMetaData> snaps = listCollectionSnapshots(collectionName);
    Optional<CollectionSnapshotMetaData> meta = Optional.empty();
    for (CollectionSnapshotMetaData m : snaps) {
      if (snapshotName.equals(m.getName())) {
        meta = Optional.of(m);
      }
    }

    if (!meta.isPresent()) {
      throw new IllegalArgumentException("The snapshot named " + snapshotName
          + " is not found for collection " + collectionName);
    }

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    for (Slice s : collectionState.getSlices()) {
      List<CoreSnapshotMetaData> replicaSnaps = meta.get().getReplicaSnapshotsForShard(s.getName());
      // Prepare a list of *existing* replicas (since one or more replicas could have been deleted after the snapshot creation).
      List<CoreSnapshotMetaData> availableReplicas = new ArrayList<>();
      for (CoreSnapshotMetaData m : replicaSnaps) {
        if (isReplicaAvailable(s, m.getCoreName())) {
          availableReplicas.add(m);
        }
      }

      if (availableReplicas.isEmpty()) {
        throw new IllegalArgumentException(
            "The snapshot named " + snapshotName + " not found for shard "
                + s.getName() + " of collection " + collectionName);
      }

      // Prefer a leader replica (at the time when the snapshot was created).
      CoreSnapshotMetaData coreSnap = availableReplicas.get(0);
      for (CoreSnapshotMetaData m : availableReplicas) {
        if (m.isLeader()) {
          coreSnap = m;
        }
      }

      String indexDirPath = coreSnap.getIndexDirPath();
      if (pathPrefix.isPresent()) {
        // If the path prefix is specified, rebuild the path to the index directory.
        Path t = new Path(coreSnap.getIndexDirPath());
        indexDirPath = (new Path(pathPrefix.get(), t.toUri().getPath())).toString();
      }

      List<String> paths = new ArrayList<>();
      for (String fileName : coreSnap.getFiles()) {
        Path p = new Path(indexDirPath, fileName);
        paths.add(p.toString());
      }

      result.put(s.getName(), paths);
    }

    return result;
  }

  public void buildCopyListings(String collectionName, String snapshotName, String localFsPath, Optional<String> pathPrefix)
      throws SolrServerException, IOException {
    Map<String, List<String>> paths = getIndexFilesPathForSnapshot(collectionName, snapshotName, pathPrefix);
    for (Map.Entry<String,List<String>> entry : paths.entrySet()) {
      StringBuilder filesBuilder = new StringBuilder();
      for (String filePath : entry.getValue()) {
        filesBuilder.append(filePath);
        filesBuilder.append("\n");
      }

      String files = filesBuilder.toString().trim();
      try (Writer w = new OutputStreamWriter(new FileOutputStream(new File(localFsPath, entry.getKey())), StandardCharsets.UTF_8)) {
        w.write(files);
      }
    }
  }

  public void backupCollectionMetaData(String collectionName, String snapshotName, String backupLoc) throws SolrServerException, IOException {
    // Backup the collection meta-data
    CollectionAdminRequest.Backup backup = new CollectionAdminRequest.Backup(collectionName, snapshotName);
    backup.setIndexBackupStrategy(CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY);
    backup.setLocation(backupLoc);
    CollectionAdminResponse resp = backup.process(solrClient);
    Preconditions.checkState(resp.getStatus() == 0, "The request failed. The status code is " + resp.getStatus());
  }

  public void prepareForExport(String collectionName, String snapshotName, String localFsPath, Optional<String> pathPrefix, String destPath) {
    try {
      buildCopyListings(collectionName, snapshotName, localFsPath, pathPrefix);
      System.out.println("Successfully prepared copylisting for the snapshot export.");
    } catch (Exception e) {
      log.error("Failed to prepare a copylisting for snapshot with name " + snapshotName + " for collection "
      + collectionName, e);
      System.out.println("Failed to prepare a copylisting for snapshot with name " + snapshotName + " for collection "
      + collectionName + " due to following error : " + e.getLocalizedMessage());
      System.exit(1);
    }

    try {
      backupCollectionMetaData(collectionName, snapshotName, destPath);
      System.out.println("Successfully backed up collection meta-data");
    } catch (Exception e) {
      log.error("Failed to backup collection meta-data for collection " + collectionName, e);
      System.out.println("Failed to backup collection meta-data for collection " + collectionName
          + " due to following error : " + e.getLocalizedMessage());
      System.exit(1);
    }
  }

  public void exportSnapshot(String collectionName, String snapshotName, String destPath, Optional<String> backupRepo,
      Optional<String> asyncReqId) {
    try {
      CollectionAdminRequest.Backup backup = new CollectionAdminRequest.Backup(collectionName, snapshotName);
      backup.setIndexBackupStrategy(CollectionAdminParams.COPY_FILES_STRATEGY);
      backup.setLocation(destPath);
      if (backupRepo.isPresent()) {
        backup.setRepositoryName(backupRepo.get());
      }
      if (asyncReqId.isPresent()) {
        backup.setAsyncId(asyncReqId.get());
      }
      CollectionAdminResponse resp = backup.process(solrClient);
      Preconditions.checkState(resp.getStatus() == 0, "The request failed. The status code is " + resp.getStatus());
    } catch (Exception e) {
      log.error("Failed to backup collection meta-data for collection " + collectionName, e);
      System.out.println("Failed to backup collection meta-data for collection " + collectionName
          + " due to following error : " + e.getLocalizedMessage());
      System.exit(1);
    }
  }

  public static void main(String[] args) throws IOException {
    CommandLineParser parser = new PosixParser();
    Options options = new Options();

    options.addOption(null, CREATE, true, "This command will create a snapshot with the specified name");
    options.addOption(null, DELETE, true, "This command will delete a snapshot with the specified name");
    options.addOption(null, LIST, false, "This command will list all the named snapshots for the specified collection.");
    options.addOption(null, DESCRIBE, true, "This command will print details for a named snapshot for the specified collection.");
    options.addOption(null, PREPARE_FOR_EXPORT, true, "This command will prepare copylistings for the specified snapshot."
        + " This command should only be used only if Solr is deployed with Hadoop and collection index files are stored on a shared"
        + " file-system e.g. HDFS");
    options.addOption(null, EXPORT_SNAPSHOT, true, "This command will create a backup for the specified snapshot.");
    options.addOption(null, HELP, false, "This command will print the help message for the snapshots related commands.");
    options.addOption(TEMP_DIR, true, "This parameter specifies the path of a temporary directory on local filesystem"
        + " during prepare-snapshot-export command.");
    options.addOption(DEST_DIR, true, "This parameter specifies the path on shared file-system (e.g. HDFS) where the snapshot related"
        + " information should be stored.");
    options.addOption(COLLECTION, true, "This parameter specifies the name of the collection to be used during snapshot operation");
    options.addOption(SOLR_ZK_ENSEMBLE, true, "This parameter specifies the Solr Zookeeper ensemble address");
    options.addOption(HDFS_PATH_PREFIX, true, "This parameter specifies the HDFS URI prefix to be used"
        + " during snapshot export preparation. This is applicable only if the Solr collection index files are stored on HDFS.");
    options.addOption(BACKUP_REPO_NAME, true, "This parameter specifies the name of the backup repository to be used"
        + " during snapshot export preparation");
    options.addOption(ASYNC_REQ_ID, true, "This parameter specifies the async request identifier to be used"
        + " during snapshot export preparation");

    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getLocalizedMessage());
      printHelp(options);
      System.exit(1);
    }

    if (cmd.hasOption(CREATE) || cmd.hasOption(DELETE) || cmd.hasOption(LIST) || cmd.hasOption(DESCRIBE)
        || cmd.hasOption(PREPARE_FOR_EXPORT) || cmd.hasOption(EXPORT_SNAPSHOT)) {
      try (SolrSnapshotsTool tool = new SolrSnapshotsTool(cmd.getOptionValue(SOLR_ZK_ENSEMBLE))) {
        if (cmd.hasOption(CREATE)) {
          String snapshotName = cmd.getOptionValue(CREATE);
          String collectionName = cmd.getOptionValue(COLLECTION);
          tool.createSnapshot(collectionName, snapshotName);

        } else if (cmd.hasOption(DELETE)) {
          String snapshotName = cmd.getOptionValue(DELETE);
          String collectionName = cmd.getOptionValue(COLLECTION);
          tool.deleteSnapshot(collectionName, snapshotName);

        } else if (cmd.hasOption(LIST)) {
          String collectionName = cmd.getOptionValue(COLLECTION);
          tool.listSnapshots(collectionName);

        } else if (cmd.hasOption(DESCRIBE)) {
          String snapshotName = cmd.getOptionValue(DESCRIBE);
          String collectionName = cmd.getOptionValue(COLLECTION);
          tool.describeSnapshot(collectionName, snapshotName);

        } else if (cmd.hasOption(PREPARE_FOR_EXPORT)) {
          String snapshotName = cmd.getOptionValue(PREPARE_FOR_EXPORT);
          String collectionName = cmd.getOptionValue(COLLECTION);
          String localFsDir = requiredArg(options, cmd, TEMP_DIR);
          String hdfsOpDir = requiredArg(options, cmd, DEST_DIR);
          Optional<String> pathPrefix = Optional.ofNullable(cmd.getOptionValue(HDFS_PATH_PREFIX));

          if (pathPrefix.isPresent()) {
            try {
              new URI(pathPrefix.get());
            } catch (URISyntaxException e) {
              System.out.println(
                  "The specified File system path prefix " + pathPrefix.get()
                      + " is invalid. The error is " + e.getLocalizedMessage());
              System.exit(1);
            }
          }
          tool.prepareForExport(collectionName, snapshotName, localFsDir, pathPrefix, hdfsOpDir);

        }  else if (cmd.hasOption(EXPORT_SNAPSHOT)) {
          String snapshotName = cmd.getOptionValue(EXPORT_SNAPSHOT);
          String collectionName = cmd.getOptionValue(COLLECTION);
          String destDir = requiredArg(options, cmd, DEST_DIR);
          Optional<String> backupRepo = Optional.ofNullable(cmd.getOptionValue(BACKUP_REPO_NAME));
          Optional<String> asyncReqId = Optional.ofNullable(cmd.getOptionValue(ASYNC_REQ_ID));

          tool.exportSnapshot(collectionName, snapshotName, destDir, backupRepo, asyncReqId);
        }
      }
    } else if (cmd.hasOption(HELP))  {
      printHelp(options);
    } else {
      System.out.println("Unknown command specified.");
      printHelp(options);
    }
  }

  private static String requiredArg(Options options, CommandLine cmd, String optVal) {
    if (!cmd.hasOption(optVal)) {
      System.out.println("Please specify the value for option " + optVal);
      printHelp(options);
      System.exit(1);
    }
    return cmd.getOptionValue(optVal);
  }

  private static boolean isReplicaAvailable (Slice s, String coreName) {
    for (Replica r: s.getReplicas()) {
      if (coreName.equals(r.getCoreName())) {
        return true;
      }
    }
    return false;
  }

  private Collection<CollectionSnapshotMetaData> listCollectionSnapshots(String collectionName)
      throws SolrServerException, IOException {
    CollectionAdminRequest.ListSnapshots listSnapshots = new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp = listSnapshots.process(solrClient);

    Preconditions.checkState(resp.getStatus() == 0);

    NamedList apiResult = (NamedList) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);

    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    for (int i = 0; i < apiResult.size(); i++) {
      result.add(new CollectionSnapshotMetaData((NamedList<Object>)apiResult.getVal(i)));
    }

    return result;
  }

  private static void printHelp(Options options) {
    StringBuilder helpFooter = new StringBuilder();
    helpFooter.append("Examples: \n");
    helpFooter.append("snapshotscli.sh --create snapshot-1 -c books -z localhost:2181 \n");
    helpFooter.append("snapshotscli.sh --list -c books -z localhost:2181 \n");
    helpFooter.append("snapshotscli.sh --describe snapshot-1 -c books -z localhost:2181 \n");
    helpFooter.append("snapshotscli.sh --export snapshot-1 -c books -z localhost:2181 -b repo -l backupPath -i req_0 \n");
    helpFooter.append("snapshotscli.sh --delete snapshot-1 -c books -z localhost:2181 \n");

    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(new OptionComarator<>());
    formatter.printHelp("SolrSnapshotsTool", null, options, helpFooter.toString(), false);
  }

  private static class OptionComarator<T extends Option> implements Comparator<T> {

    public int compare(T o1, T o2) {
      String s1 = o1.hasLongOpt() ? o1.getLongOpt() : o1.getOpt();
      String s2 = o2.hasLongOpt() ? o2.getLongOpt() : o2.getOpt();
        return OPTION_HELP_ORDER.indexOf(s1) - OPTION_HELP_ORDER.indexOf(s2);
    }
}

}
