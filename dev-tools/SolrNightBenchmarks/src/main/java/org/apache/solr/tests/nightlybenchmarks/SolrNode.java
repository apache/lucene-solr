package org.apache.solr.tests.nightlybenchmarks;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

enum SolrNodeAction {
	NODE_START, NODE_STOP
}

public class SolrNode {

	final static Logger logger = Logger.getLogger(SolrNode.class);
	public static final String URL_BASE = "http://archive.apache.org/dist/lucene/solr/";

	private String nodeDirectory;
	public String port;
	private String commitId;
	private String zooKeeperIp;
	private String zooKeeperPort;
	public String collectionName;
	public String baseDirectory;
	public boolean isRunningInCloudMode;

	private String gitDirectoryPath = Util.DOWNLOAD_DIR + "git-repository-";

	public SolrNode(String commitId, String zooKeeperIp, String zooKeeperPort, boolean isRunningInCloudMode)
			throws IOException, GitAPIException {
		super();
		this.commitId = commitId;
		this.zooKeeperIp = zooKeeperIp;
		this.zooKeeperPort = zooKeeperPort;
		this.isRunningInCloudMode = isRunningInCloudMode;
		this.gitDirectoryPath = Util.DOWNLOAD_DIR + "git-repository-" + commitId;
		Util.GIT_REPOSITORY_PATH = this.gitDirectoryPath;
		this.install();
	}

	private void install() throws IOException, GitAPIException {

		Util.postMessage("** Installing Solr Node ...", MessageType.WHITE_TEXT, true);
		this.port = String.valueOf(Util.getFreePort());

		this.baseDirectory = Util.SOLR_DIR + UUID.randomUUID().toString() + File.separator;

		this.nodeDirectory = this.baseDirectory + "solr-7.0.0-SNAPSHOT/bin/";

		try {

			Util.postMessage("** Checking if SOLR node directory exists ...", MessageType.WHITE_TEXT, true);
			File node = new File(nodeDirectory);

			if (!node.exists()) {

				Util.postMessage("Node directory does not exist, creating it ...", MessageType.WHITE_TEXT, true);
				node.mkdir();
				Util.postMessage("Directory Created: " + nodeDirectory, MessageType.GREEN_TEXT, true);

			}

		} catch (Exception e) {

			Util.postMessage(e.getMessage(), MessageType.RED_TEXT, true);

		}

		this.checkoutCommitAndBuild();
		Util.extract(Util.DOWNLOAD_DIR + "solr-" + commitId + ".zip", baseDirectory);
	}

	void checkoutCommitAndBuild() throws IOException, GitAPIException {
		Util.postMessage("** Checking out Solr: " + commitId + " ...", MessageType.WHITE_TEXT, true);

		File gitDirectory = new File(gitDirectoryPath);

		Git repository;

		if (gitDirectory.exists()) {
			repository = Git.open(gitDirectory);
			repository.checkout().setName(commitId).call();
		} else {
			BenchmarkAppConnector.writeToWebAppDataFile("iamcloning.txt", "", true, FileType.IS_CLONING_FILE);
					repository = Git.cloneRepository().setURI(Util.LUCENE_SOLR_REPOSITORY_URL)
							.setDirectory(gitDirectory).call();
					repository.checkout().setName(commitId).call();
			BenchmarkAppConnector.deleteFolder(FileType.IS_CLONING_FILE);
		}

		String packageFilename = gitDirectoryPath + "/solr/package/solr-7.0.0-SNAPSHOT.zip";
		String tarballLocation = Util.DOWNLOAD_DIR + "solr-" + commitId + ".zip";

		if (new File(tarballLocation).exists() == false) {
			if (new File(packageFilename).exists() == false) {
				Util.postMessage("** There were new changes, need to rebuild ...", MessageType.WHITE_TEXT, true);
				Util.execute("ant ivy-bootstrap", gitDirectoryPath);
				// Util.execute("ant compile", gitDirectoryPath);
				Util.execute("ant package", gitDirectoryPath + File.separator + "solr");
			}

			if (new File(packageFilename).exists()) {
				System.out.println("Trying to copy: " + packageFilename + " to " + tarballLocation);
				Files.copy(Paths.get(packageFilename), Paths.get(tarballLocation));
				System.out.println("File copied!");
			} else {
				throw new IOException("Couldn't build the package"); // nocommit
																		// fix,
																		// better
																		// exception
			}
		}

		Util.postMessage(
				"** Do we have packageFilename? " + (new File(tarballLocation).exists() ? "yes" : "no") + " ...",
				MessageType.WHITE_TEXT, true);
	}

	public int doAction(SolrNodeAction action) {

		long start = 0;
		long end = 0;
		int returnValue = 0;

		start = System.currentTimeMillis();
		new File(nodeDirectory + "solr").setExecutable(true);
		if (action == SolrNodeAction.NODE_START) {

			if (isRunningInCloudMode) {
				returnValue = Util.execute(nodeDirectory + "solr start -force " + "-p " + port + " -m 4g " + " -z "
						+ zooKeeperIp + ":" + zooKeeperPort, nodeDirectory);
			} else {
				returnValue = Util.execute(nodeDirectory + "solr start -force " + "-p " + port + " -m 4g ",
						nodeDirectory);
			}

		} else if (action == SolrNodeAction.NODE_STOP) {

			if (isRunningInCloudMode) {
				returnValue = Util.execute(
						nodeDirectory + "solr stop -p " + port + " -z " + zooKeeperIp + ":" + zooKeeperPort + " -force",
						nodeDirectory);
			} else {
				returnValue = Util.execute(nodeDirectory + "solr stop -p " + port + " -force", nodeDirectory);
			}

		}
		end = System.currentTimeMillis();
		Util.postMessage("** Time taken for the node " + action + " activity is: " + (end - start) + " millisecond(s)",
				MessageType.RED_TEXT, false);

		return returnValue;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> createCollection(String coreName, String collectionName)
			throws IOException, InterruptedException {

		Thread thread = new Thread(
				new MetricCollector(this.commitId, TestType.STANDALONE_CREATE_COLLECTION, this.port));
		thread.start();

		this.collectionName = collectionName;
		Util.postMessage("** Creating core ... ", MessageType.WHITE_TEXT, true);

		long start;
		long end;
		int returnVal;

		start = System.currentTimeMillis();
		returnVal = Util.execute(
				"./solr create_core -c " + coreName + " -p " + port + " -collection " + collectionName + " -force",
				nodeDirectory);
		end = System.currentTimeMillis();


		Util.postMessage("** Time for creating the core is: " + (end - start) + " millisecond(s)", MessageType.RED_TEXT,
				false);

		thread.stop();

		Map<String, String> returnMap = new HashMap<String, String>();

		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		returnMap.put("ProcessExitValue", "" + returnVal);
		returnMap.put("TimeStamp", "" + ft.format(dNow));
		returnMap.put("CreateCollectionTime", "" + ((double) (end - start)));
		returnMap.put("CommitID", this.commitId);

		return returnMap;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> createCollection(String collectionName, String configName, String shards,
			String replicationFactor) throws IOException, InterruptedException {

		Thread thread = new Thread(new MetricCollector(this.commitId, TestType.CLOUD_CREATE_COLLECTION, this.port));
		thread.start();

		this.collectionName = collectionName;
		Util.postMessage("** Creating collection ... ", MessageType.WHITE_TEXT, true);

		long start;
		long end;
		int returnVal;

		if (configName != null) {
			start = System.currentTimeMillis();
			returnVal = Util.execute("./solr create_collection -collection " + collectionName + " -shards " + shards
					+ " -n " + configName + " -replicationFactor " + replicationFactor + " -force", nodeDirectory);
			end = System.currentTimeMillis();
		} else {
			start = System.currentTimeMillis();
			returnVal = Util.execute("./solr create_collection -collection " + collectionName + " -shards " + shards
					+ " -replicationFactor " + replicationFactor + " -force", nodeDirectory);
			end = System.currentTimeMillis();
		}

		Util.postMessage("** Time for creating the collection is: " + (end - start) + " millisecond(s)",
				MessageType.RED_TEXT, false);

		thread.stop();

		Map<String, String> returnMap = new HashMap<String, String>();

		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		returnMap.put("ProcessExitValue", "" + returnVal);
		returnMap.put("TimeStamp", "" + ft.format(dNow));
		returnMap.put("CreateCollectionTime", "" + ((double) (end - start)));
		returnMap.put("CommitID", this.commitId);

		return returnMap;
	}
	
	
	public int deleteCollection(String collectionName) throws IOException, InterruptedException {

		this.collectionName = collectionName;
		Util.postMessage("** Deleting collection ... ", MessageType.RED_TEXT, true);
		
		return Util.execute("./solr delete -c " + collectionName + " -deleteConfig true", nodeDirectory);

	}


	public String getNodeDirectory() {
		return nodeDirectory;
	}

	public String getBaseUrl() {
		return "http://localhost:" + port + "/solr/";
	}

	public String getBaseUrlC() {
		return "http://localhost:" + port + "/solr/" + this.collectionName;
	}

	public void cleanup() {
		Util.execute("rm -r -f " + baseDirectory, baseDirectory);
	}
}