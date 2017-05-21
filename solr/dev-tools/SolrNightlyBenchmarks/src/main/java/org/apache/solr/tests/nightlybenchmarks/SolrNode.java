package org.apache.solr.tests.nightlybenchmarks;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

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

	private String gitDirectoryPath = Util.TEMP_DIR + "git-repository-";

	public SolrNode(String commitId, String zooKeeperIp, String zooKeeperPort, boolean isRunningInCloudMode) throws IOException, GitAPIException {
		super();
		this.commitId = commitId;
		this.zooKeeperIp = zooKeeperIp;
		this.zooKeeperPort = zooKeeperPort;		
		this.isRunningInCloudMode = isRunningInCloudMode;
		this.gitDirectoryPath = Util.TEMP_DIR + "git-repository-" + commitId;		
		Util.gitRepositoryPath = this.gitDirectoryPath;
		this.install();
	}

	private void install() throws IOException, GitAPIException {

		Util.postMessage("** Installing Solr Node ...", MessageType.ACTION, true);
		this.port = String.valueOf(Util.getFreePort());
		
		  this.baseDirectory  = Util.BASE_DIR + UUID.randomUUID().toString() + File.separator;
		  
		  this.nodeDirectory = this.baseDirectory + "solr-7.0.0-SNAPSHOT/bin/";
		  
		  try {
		  
		  Util.postMessage("** Checking if SOLR node directory exists ...",
		  MessageType.ACTION, true); File node = new File(nodeDirectory);
		  
		  if (!node.exists()) {
		  
		  Util.postMessage("Node directory does not exist, creating it ...",
		  MessageType.ACTION, true); node.mkdir();
		  Util.postMessage("Directory Created: " + nodeDirectory,
		  MessageType.RESULT_SUCCESS, true);
		  
		  }
		  
		  } catch (Exception e) {
		  
		  Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);
		  
		  }
		 
		this.checkoutCommitAndBuild();  
		Util.extract(Util.TEMP_DIR + "solr-" + commitId + ".zip", baseDirectory);		  
	}

	void checkoutCommitAndBuild() throws IOException, GitAPIException {
		Util.postMessage("** Checking out Solr: " + commitId + " ...", MessageType.ACTION, true);

		File gitDirectory = new File(gitDirectoryPath);

		Git repository;

		if (gitDirectory.exists()) {
			repository = Git.open(gitDirectory);

			repository.checkout().setName(commitId).call();

		} else {
			repository = Git.cloneRepository().setURI("https://github.com/apache/lucene-solr")
					.setDirectory(gitDirectory).call();
			repository.checkout().setName(commitId).call();
		}

		String packageFilename = gitDirectoryPath + "/solr/package/solr-7.0.0-SNAPSHOT.zip";
		String tarballLocation = Util.TEMP_DIR + "solr-" + commitId + ".zip";

		if (new File(tarballLocation).exists() == false) {
			if (new File(packageFilename).exists() == false) {
				Util.postMessage("** There were new changes, need to rebuild ...", MessageType.ACTION, true);
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
				MessageType.ACTION, true);
	}

	@SuppressWarnings("finally")
	public int start() {

		Util.postMessage("** Starting Solr Node ...", MessageType.ACTION, true);

		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;
		long start = 0;
		long end = 0;
		
		try {

			if (isRunningInCloudMode) {
				start = System.nanoTime();

				new File(nodeDirectory + "solr").setExecutable(true);
				proc = rt.exec(nodeDirectory + "solr start -force " + "-p " + port + " -m 4g" + " -z " + zooKeeperIp
						+ ":" + zooKeeperPort);
				end = System.nanoTime();
					
			} else {

				new File(nodeDirectory + "solr").setExecutable(true);
				 start = System.nanoTime();
				proc = rt.exec(nodeDirectory + "solr start -force " + "-p " + port + " -m 4g");
				end = System.nanoTime();

			}

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();
			
			Util.postMessage("** Time taken to start the Solr Node is: " + (end-start) + " nanosecond(s)", MessageType.RESULT_ERRROR, false);
			return proc.exitValue();

		} catch (Exception e) {

			Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);
			return -1;

		} finally {

			return proc.exitValue();

		}

	}

	public void stop() {

		Util.postMessage("** Stopping Solr Node ...", MessageType.ACTION, true);		long start = 0;
		long end = 0;
		

		if (isRunningInCloudMode) {
			start = System.nanoTime();

		 Util.execute(
					nodeDirectory + "solr stop -p " + port + " -z " + zooKeeperIp + ":" + zooKeeperPort + " -force",
					nodeDirectory);
			
		 end = System.nanoTime();

		} else  {
			start = System.nanoTime();

		 Util.execute(nodeDirectory + "solr stop -p " + port + " -force", nodeDirectory);			end = System.nanoTime();
	    }
		Util.postMessage("** Time taken to stop the node is: " + (end-start) + " nanosecond(s)", MessageType.RESULT_ERRROR, false);		
			
		
		}

	
	@SuppressWarnings("deprecation")
	public void createCore(String coreName, String collectionName) throws IOException, InterruptedException {

        Thread thread =new Thread(new MetricEstimation(this.commitId, TestType.STANDALONE_CREATE_COLLECTION, this.port));  
        thread.start();  
	
		this.collectionName = collectionName;
		Util.postMessage("** Creating core ... ", MessageType.ACTION, true);

		long start;
		long end;
		
		start = System.nanoTime();
		Util.execute("./solr create_core -c " + coreName + " -p " + port + " -collection " + collectionName + " -force", nodeDirectory);
		end = System.nanoTime();
		
		Util.postMessage("** Time for creating the core is: " + (end-start) + " nanosecond(s)", MessageType.RESULT_ERRROR, false);
		
         Date dNow = new Date( );
		 SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
		
		BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_standalone_regular.csv", ft.format(dNow) + ", " + ((end-start)/1000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.STANDALONE_CREATE_COLLECTION_MAIN);	
		
		thread.stop();
	}
	
	@SuppressWarnings("deprecation")
	public int createCollection(String collectionName, String configName, String shards, String replicationFactor)
			throws IOException, InterruptedException {

        Thread thread =new Thread(new MetricEstimation(this.commitId, TestType.CLOUD_CREATE_COLLECTION, this.port));  
        thread.start();  
	
		this.collectionName = collectionName;
		Util.postMessage("** Creating collection ... ", MessageType.ACTION, true);

		long start;
		long end;
		int returnVal;
		
		if (configName != null) {
				start = System.nanoTime();
				returnVal = Util.execute("./solr create_collection -collection "
						+ collectionName + " -shards " + shards
						+ " -n " + configName 
						+ " -replicationFactor " + replicationFactor + " -force", nodeDirectory);
				end = System.nanoTime();
		} else {
				start = System.nanoTime();
				returnVal = Util.execute("./solr create_collection -collection "
						+ collectionName + " -shards " + shards
						+ " -replicationFactor " + replicationFactor + " -force", nodeDirectory);
				end = System.nanoTime();
		}
		
		Util.postMessage("** Time for creating the collection is: " + (end-start) + " nanosecond(s)", MessageType.RESULT_ERRROR, false);
		
        Date dNow = new Date( );
		SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
		
		BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_cloud_regular.csv", ft.format(dNow) + ", " + ((end-start)/1000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.CLOUD_CREATE_COLLECTION_MAIN);	
		
		thread.stop();		
		return returnVal;
	}


	public String getNodeDirectory() {
		return nodeDirectory;
	}

	public String getBaseUrl() {
		return "http://localhost:" + port + "/solr/";
	}
	
	public void cleanup() {
		try {
			Util.deleteDirectory(baseDirectory);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}