/*
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

package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.ZipInputStream;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

enum MessageType {
	YELLOW_TEXT, WHITE_TEXT, GREEN_TEXT, RED_TEXT, BLUE_TEXT, BLACK_TEXT, PURPLE_TEXT, CYAN_TEXT
};

/**
 * This class provides utility methods for the package.
 * @author Vivek Narang
 *
 */
public class Util {
	
	public final static Logger logger = Logger.getLogger(Util.class);

	public static String WORK_DIRECTORY = System.getProperty("user.dir");
	public static String DNAME = "SolrNightlyBenchmarksWorkDirectory";
	public static String BASE_DIR = WORK_DIRECTORY + File.separator + DNAME + File.separator;
	public static String RUN_DIR = BASE_DIR + "RunDirectory" + File.separator;
	public static String DOWNLOAD_DIR = BASE_DIR + "Download" + File.separator;
	public static String ZOOKEEPER_DOWNLOAD_URL = "http://www.us.apache.org/dist/zookeeper/";
	public static String ZOOKEEPER_RELEASE = "3.4.6";
	public static String ZOOKEEPER_DIR = RUN_DIR;
	public static String SOLR_DIR = RUN_DIR;
	public static String ZOOKEEPER_IP = "127.0.0.1";
	public static String ZOOKEEPER_PORT = "2181";
	public static String LUCENE_SOLR_REPOSITORY_URL = "https://github.com/apache/lucene-solr";
	public static String GIT_REPOSITORY_PATH;
	public static String SOLR_PACKAGE_DIR;
	public static String SOLR_PACKAGE_DIR_LOCATION;
	public static String COMMIT_ID;
	public static String TEST_ID = UUID.randomUUID().toString();
	public static String TEST_TIME = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());
	public static String METRIC_ESTIMATION_PERIOD = "1000";
	public static String QUERY_THREAD_COUNT_FIRST = "";
	public static String QUERY_THREAD_COUNT_SECOND = "";
	public static String QUERY_THREAD_COUNT_THIRD = "";
	public static String QUERY_THREAD_COUNT_FOURTH = "";
	public static String TEST_DATA_DIRECTORY = "";
	public static String ONEM_TEST_DATA = "";
	public static String NUMERIC_QUERY_TERM_DATA = "";
	public static String NUMERIC_QUERY_PAIR_DATA = "";
	public static String NUMERIC_QUERY_AND_OR_DATA = "";
	public static String NUMERIC_SORTED_QUERY_PAIR_DATA = "";
	public static String TEXT_TERM_DATA = "";
	public static String TEXT_PHRASE_DATA = "";
	public static String HIGHLIGHT_TERM_DATA = "";
	public static String TEST_DATA_STORE_LOCATION = "";
	public static String RANGE_FACET_DATA = "";
	public static String TEST_DATA_ARCHIVE_LOCATION = "";
	public static long NUMBER_OF_QUERIES_TO_RUN = 1000;
	public static long NUMBER_OF_QUERIES_TO_RUN_FOR_FACETING = 1000;
	public static long TEST_WITH_NUMBER_OF_DOCUMENTS = 100000;
	public static boolean USE_COLORED_TEXT_ON_CONSOLE = true;
	public static boolean SILENT = false;
	public static List<String> argsList;


	/**
	 * A method used for invoking a process with specific parameters.
	 * 
	 * @param command
	 * @param workingDirectoryPath
	 * @return
	 * @throws Exception 
	 */
	public static int execute(String command, String workingDirectoryPath) throws Exception {
		logger.debug("Executing: " + command);
		logger.debug("Working dir: " + workingDirectoryPath);
		File workingDirectory = new File(workingDirectoryPath);

		workingDirectory.setExecutable(true);

		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader processErrorStream = null;
		ProcessStreamReader processOutputStream = null;

		try {
			proc = rt.exec(command, new String[] {}, workingDirectory);

			processErrorStream = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			processOutputStream = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			processErrorStream.start();
			processOutputStream.start();
			proc.waitFor();
			return proc.exitValue();
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * A method for printing output on a single line.
	 * 
	 * @param message
	 */
	public static void postMessageOnLine(String message) {
		if (!SILENT) {
			System.out.print(message);
		}
	}

	/**
	 * A method used for checking if the required directories are present or
	 * not. If not this method creates the required directories.
	 * 
	 * @throws IOException
	 */
	public static void checkBaseAndTempDir() throws IOException {

		File webAppDir = new File(BenchmarkAppConnector.benchmarkAppDirectory);
		if (!webAppDir.exists()) {
			webAppDir.mkdirs();
		}

		File dataWebAppDir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data/");
		if (!dataWebAppDir.exists()) {
			dataWebAppDir.mkdirs();
		}

		File cloningDataWebAppDir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data/cloning/");
		if (!cloningDataWebAppDir.exists()) {
			cloningDataWebAppDir.mkdirs();
		}

		File commitQueueDataWebAppDir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data/commit_queue/");
		if (!commitQueueDataWebAppDir.exists()) {
			commitQueueDataWebAppDir.mkdirs();
		}

		File lastrunDataWebAppDir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data/lastrun/");
		if (!lastrunDataWebAppDir.exists()) {
			lastrunDataWebAppDir.mkdirs();
		}

		File runningDataWebAppDir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data/running/");
		if (!runningDataWebAppDir.exists()) {
			runningDataWebAppDir.mkdirs();
		}

		BasicConfigurator.configure();
		File baseDirectory = new File(BASE_DIR);
		if (!baseDirectory.exists()) {
			baseDirectory.mkdirs();
		}
		File tempDirectory = new File(DOWNLOAD_DIR);
		if (!tempDirectory.exists()) {
			tempDirectory.mkdir();
		}
		File runDirectory = new File(RUN_DIR);
		if (!runDirectory.exists()) {
			runDirectory.mkdir();
		}
		
		// Marking for GC
		baseDirectory = null;
		tempDirectory = null;
		runDirectory = null;
	}

	/**
	 * A method used for get an available free port for running the
	 * solr/zookeeper node on.
	 * 
	 * @return int
	 * @throws Exception 
	 */
	public static int getFreePort() throws Exception {

		int port = ThreadLocalRandom.current().nextInt(10000, 60000);
		logger.debug("Looking for a free port ... Checking availability of port number: " + port);
		ServerSocket serverSocket = null;
		DatagramSocket datagramSocket = null;
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);
			datagramSocket = new DatagramSocket(port);
			datagramSocket.setReuseAddress(true);
			logger.debug("Port " + port + " is free to use. Using this port !!");
			return port;
		} catch (IOException e) {
		} finally {
			if (datagramSocket != null) {
				datagramSocket.close();
			}

			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
					throw new IOException(e.getMessage());
				}
			}
			// Marking for GC
			serverSocket = null;
			datagramSocket = null;
		}

		logger.debug("Port " + port + " looks occupied trying another port number ... ");
		return getFreePort();
	}

	/**
	 * 
	 * @param plaintext
	 * @return String
	 * @throws Exception 
	 */
	static public String md5(String plaintext) throws Exception {
		MessageDigest m;
		String hashtext = null;
		try {
			m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(plaintext.getBytes());
			byte[] digest = m.digest();
			BigInteger bigInt = new BigInteger(1, digest);
			hashtext = bigInt.toString(16);
			// Now we need to zero pad it if you actually want the full 32
			// chars.
			while (hashtext.length() < 32) {
				hashtext = "0" + hashtext;
			}
		} catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return hashtext;
	}

	/**
	 * A metod used for extracting files from an archive.
	 * 
	 * @param zipIn
	 * @param filePath
	 * @throws Exception 
	 */
	public static void extractFile(ZipInputStream zipIn, String filePath) throws Exception {

		BufferedOutputStream bos = null;
		try {

			bos = new BufferedOutputStream(new FileOutputStream(filePath));
			byte[] bytesIn = new byte[4096];
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
			bos.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			bos.close();
			// Marking for GC
			bos = null;
		}
	}

	/**
	 * A method used for downloading a resource from external sources.
	 * 
	 * @param downloadURL
	 * @param fileDownloadLocation
	 * @throws Exception 
	 */
	public static void download(String downloadURL, String fileDownloadLocation) throws Exception {

		URL link = null;
		InputStream in = null;
		FileOutputStream fos = null;

		try {

			link = new URL(downloadURL);
			in = new BufferedInputStream(link.openStream());
			fos = new FileOutputStream(fileDownloadLocation);
			byte[] buf = new byte[1024 * 1024]; // 1mb blocks
			int n = 0;
			long size = 0;
			while (-1 != (n = in.read(buf))) {
				size += n;
				logger.debug("\r" + size + " ");
				fos.write(buf, 0, n);
			}
			fos.close();
			in.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * A method used for extracting files from a zip archive.
	 * 
	 * @param filename
	 * @param filePath
	 * @throws IOException
	 */
	public static void extract(String filename, String filePath) throws IOException {
		logger.debug(" Attempting to unzip the downloaded release ...");
		try {
			logger.debug("File to be copied: " + filename);
			logger.debug("Location: " + filePath);
			ZipFile zip = new ZipFile(filename);
			zip.extractAll(filePath);
		} catch (ZipException ex) {
			logger.error(ex.getMessage());
			throw new IOException(ex);
		}
	}

	/**
	 * A method used for fetching latest commit from a remote repository.
	 * 
	 * @param repositoryURL
	 * @return
	 * @throws IOException
	 */
	public static String getLatestCommitID(String repositoryURL) throws IOException {
		logger.debug(" Getting the latest commit ID from: " + repositoryURL);
		return new BufferedReader(new InputStreamReader(
				Runtime.getRuntime().exec("git ls-remote " + repositoryURL + " HEAD").getInputStream())).readLine()
						.split("HEAD")[0].trim();
	}

	/**
	 * A method used for getting the repository path.
	 * 
	 * @return
	 */
	public static String getLocalRepoPath() {
		Util.GIT_REPOSITORY_PATH = Util.DOWNLOAD_DIR + "git-repository";
		return Util.GIT_REPOSITORY_PATH;
	}

	/**
	 * A method used for getting the path for a local repository.
	 * 
	 * @return
	 */
	public static String getLocalLastRepoPath() {
		Util.GIT_REPOSITORY_PATH = Util.DOWNLOAD_DIR + "git-repository-" + BenchmarkAppConnector.getLastRunCommitID();
		return Util.GIT_REPOSITORY_PATH;
	}

	/**
	 * A method for getting the commit information and publishing it on a file.
	 * @throws Exception 
	 */
	public static void getAndPublishCommitInformation() throws Exception {
		BenchmarkAppConnector.writeToWebAppDataFile(
				Util.TEST_ID + "_" + Util.COMMIT_ID + "_COMMIT_INFORMATION_dump.csv", Util.getCommitInformation(), true,
				FileType.COMMIT_INFORMATION_FILE);
	}

	/**
	 * A method used for getting the commit information.
	 * 
	 * @return String
	 * @throws Exception 
	 */
	public static String getCommitInformation() throws Exception {
		logger.debug(" Getting the latest commit Information from local repository");
		File directory = new File(Util.getLocalRepoPath());
		directory.setExecutable(true);
		BufferedReader reader;
		String line = "";
		String returnString = "";

		try {
			reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime()
					.exec("git show --no-patch " + Util.COMMIT_ID, new String[] {}, directory).getInputStream()));

			while ((line = reader.readLine()) != null) {
				returnString += line.replaceAll("<", " ").replaceAll(">", " ").replaceAll(",", "").trim() + "<br/>";
			}

			return returnString;
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			// Marking for GC
			directory = null;
			reader = null;
			line = null;
			returnString = null;
		}

	}

	/**
	 * A method used for getting the host system information.
	 * @throws Exception 
	 */
	public static void getSystemEnvironmentInformation() throws Exception {

		logger.debug(" Getting the test environment information");

		BufferedReader reader;
		String returnString = "";
		String line = "";

		try {

			reader = new BufferedReader(
					new InputStreamReader(Runtime.getRuntime().exec("sudo lshw -short").getInputStream()));
			while ((line = reader.readLine()) != null) {
				returnString += line.replaceAll("\\p{C}", "&nbsp;") + "<br/>";
			}

			returnString += "<br/>";

			returnString += "Java Version: " + System.getProperty("java.version");

			returnString += "<br/>";

			reader = new BufferedReader(
					new InputStreamReader(Runtime.getRuntime().exec("ant -version").getInputStream()));
			while ((line = reader.readLine()) != null) {
				returnString += line.replaceAll(" ", "&nbsp;") + "<br/>";
			}

			returnString += "<br/>";

			reader = new BufferedReader(
					new InputStreamReader(Runtime.getRuntime().exec("cat /proc/version").getInputStream()));
			while ((line = reader.readLine()) != null) {
				returnString += line.replaceAll(" ", "&nbsp;") + "<br/>";
			}

			BenchmarkAppConnector.writeToWebAppDataFile("testEnv.csv", returnString, true, FileType.TEST_ENV_FILE);

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			reader = null;
			returnString = null;
			line = null;
		}
	}

	/**
	 * A method used for reading the property file and injecting data into the
	 * data variables.
	 * @throws Exception 
	 */
	public static void getPropertyValues() throws Exception {

		// THIS METHOD SHOULD BE CALLED BEFORE ANYOTHER METHOD

		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("config.properties");
			prop.load(input);

			// get the property value and print it out
			BenchmarkAppConnector.benchmarkAppDirectory = prop
					.getProperty("SolrNightlyBenchmarks.benchmarkAppDirectory");
			logger.debug(
					"Getting Property Value for benchmarkAppDirectory: " + BenchmarkAppConnector.benchmarkAppDirectory);
			SolrIndexingClient.solrCommitHistoryData = prop.getProperty("SolrNightlyBenchmarks.solrCommitHistoryData");
			logger.debug(
					"Getting Property Value for solrCommitHistoryData: " + SolrIndexingClient.solrCommitHistoryData);
			SolrIndexingClient.amazonFoodData = prop.getProperty("SolrNightlyBenchmarks.amazonFoodData");
			logger.debug("Getting Property Value for amazonFoodData: " + SolrIndexingClient.amazonFoodData);
			MetricCollector.metricsURL = prop.getProperty("SolrNightlyBenchmarks.metricsURL");
			logger.debug("Getting Property Value for metricsURL: " + MetricCollector.metricsURL);
			Util.ZOOKEEPER_DOWNLOAD_URL = prop.getProperty("SolrNightlyBenchmarks.zookeeperDownloadURL");
			logger.debug("Getting Property Value for zookeeperDownloadURL: " + Util.ZOOKEEPER_DOWNLOAD_URL);
			Util.ZOOKEEPER_RELEASE = prop.getProperty("SolrNightlyBenchmarks.zookeeperDownloadVersion");
			logger.debug("Getting Property Value for zookeeperDownloadVersion: " + Util.ZOOKEEPER_RELEASE);
			Util.ZOOKEEPER_IP = prop.getProperty("SolrNightlyBenchmarks.zookeeperHostIp");
			logger.debug("Getting Property Value for zookeeperHostIp: " + Util.ZOOKEEPER_IP);
			Util.ZOOKEEPER_PORT = prop.getProperty("SolrNightlyBenchmarks.zookeeperHostPort");
			logger.debug("Getting Property Value for zookeeperHostPort: " + Util.ZOOKEEPER_PORT);
			Util.LUCENE_SOLR_REPOSITORY_URL = prop.getProperty("SolrNightlyBenchmarks.luceneSolrRepositoryURL");
			logger.debug("Getting Property Value for luceneSolrRepositoryURL: " + Util.LUCENE_SOLR_REPOSITORY_URL);
			Util.METRIC_ESTIMATION_PERIOD = prop.getProperty("SolrNightlyBenchmarks.metricEstimationPeriod");
			logger.debug("Getting Property Value for metricEstimationPeriod: " + Util.METRIC_ESTIMATION_PERIOD);
			Util.QUERY_THREAD_COUNT_FIRST = prop.getProperty("SolrNightlyBenchmarks.queryThreadCountFirst");
			logger.debug("Getting Property Value for queryThreadCountFirst: " + Util.QUERY_THREAD_COUNT_FIRST);
			Util.QUERY_THREAD_COUNT_SECOND = prop.getProperty("SolrNightlyBenchmarks.queryThreadCountSecond");
			logger.debug("Getting Property Value for queryThreadCountSecond: " + Util.QUERY_THREAD_COUNT_SECOND);
			Util.QUERY_THREAD_COUNT_THIRD = prop.getProperty("SolrNightlyBenchmarks.queryThreadCountThird");
			logger.debug("Getting Property Value for queryThreadCountThird: " + Util.QUERY_THREAD_COUNT_THIRD);
			Util.QUERY_THREAD_COUNT_FOURTH = prop.getProperty("SolrNightlyBenchmarks.queryThreadCountFourth");
			logger.debug("Getting Property Value for queryThreadCountFourth: " + Util.QUERY_THREAD_COUNT_FOURTH);
			Util.TEST_DATA_DIRECTORY = prop.getProperty("SolrNightlyBenchmarks.testDataDirectory");
			logger.debug("Getting Property Value for testDataDirectory: " + Util.TEST_DATA_DIRECTORY);
			Util.ONEM_TEST_DATA = prop.getProperty("SolrNightlyBenchmarks.1MTestData");
			logger.debug("Getting Property Value for 1MTestData: " + Util.ONEM_TEST_DATA);
			Util.NUMERIC_QUERY_TERM_DATA = prop.getProperty("SolrNightlyBenchmarks.staticNumericQueryTermsData");
			logger.debug("Getting Property Value for staticNumericQueryTermsData: " + Util.NUMERIC_QUERY_TERM_DATA);
			Util.NUMERIC_QUERY_PAIR_DATA = prop.getProperty("SolrNightlyBenchmarks.staticNumericQueryPairsData");
			logger.debug("Getting Property Value for staticNumericQueryPairsData: " + Util.NUMERIC_QUERY_PAIR_DATA);
			Util.TEST_WITH_NUMBER_OF_DOCUMENTS = Long
					.parseLong(prop.getProperty("SolrNightlyBenchmarks.testWithNumberOfDocuments"));
			logger.debug(
					"Getting Property Value for testWithNumberOfDocuments: " + Util.TEST_WITH_NUMBER_OF_DOCUMENTS);
			Util.NUMERIC_SORTED_QUERY_PAIR_DATA = prop
					.getProperty("SolrNightlyBenchmarks.staticNumericSortedQueryPairsData");
			logger.debug("Getting Property Value for staticNumericSortedQueryPairsData: "
					+ Util.NUMERIC_SORTED_QUERY_PAIR_DATA);
			Util.USE_COLORED_TEXT_ON_CONSOLE = new Boolean(
					prop.getProperty("SolrNightlyBenchmarks.useColoredTextOnConsole"));
			logger.debug("Getting Property Value for useColoredTextOnConsole: " + Util.USE_COLORED_TEXT_ON_CONSOLE);
			Util.NUMERIC_QUERY_AND_OR_DATA = prop.getProperty("SolrNightlyBenchmarks.staticNumericQueryAndOrTermsData");
			logger.debug(
					"Getting Property Value for staticNumericQueryAndOrTermsData: " + Util.NUMERIC_QUERY_AND_OR_DATA);
			Util.TEXT_TERM_DATA = prop.getProperty("SolrNightlyBenchmarks.staticTextTermQueryData");
			logger.debug("Getting Property Value for staticTextTermQueryData: " + Util.TEXT_TERM_DATA);
			Util.TEXT_PHRASE_DATA = prop.getProperty("SolrNightlyBenchmarks.staticTextPhraseQueryData");
			logger.debug("Getting Property Value for staticTextPhraseQueryData: " + Util.TEXT_PHRASE_DATA);
			Util.HIGHLIGHT_TERM_DATA = prop.getProperty("SolrNightlyBenchmarks.highlightTermsData");
			logger.debug("Getting Property Value for highlightTermsData: " + Util.HIGHLIGHT_TERM_DATA);
			Util.TEST_DATA_STORE_LOCATION = prop.getProperty("SolrNightlyBenchmarks.testDataStoreURL");
			logger.debug("Getting Property Value for testDataStoreURL: " + Util.TEST_DATA_STORE_LOCATION);
			Util.RANGE_FACET_DATA = prop.getProperty("SolrNightlyBenchmarks.rangeFacetTestData");
			logger.debug("Getting Property Value for rangeFacetTestData: " + Util.RANGE_FACET_DATA);
			Util.TEST_DATA_ARCHIVE_LOCATION = prop.getProperty("SolrNightlyBenchmarks.testDataArchiveLocation");
			logger.debug("Getting Property Value for testDataArchiveLocation: " + Util.TEST_DATA_ARCHIVE_LOCATION);
			Util.NUMBER_OF_QUERIES_TO_RUN = Long.parseLong(prop.getProperty("SolrNightlyBenchmarks.numberOfQueriesToRun"));
			logger.debug("Getting Property Value for numberOfQueriesToRun: " + Util.NUMBER_OF_QUERIES_TO_RUN);
			Util.NUMBER_OF_QUERIES_TO_RUN_FOR_FACETING = Long.parseLong(prop.getProperty("SolrNightlyBenchmarks.numberOfQueriesToRunForFaceting"));
			logger.debug("Getting Property Value for numberOfQueriesToRunForFaceting: " + Util.NUMBER_OF_QUERIES_TO_RUN_FOR_FACETING);
			
			if (BenchmarkAppConnector.benchmarkAppDirectory
					.charAt(BenchmarkAppConnector.benchmarkAppDirectory.length() - 1) != File.separator.charAt(0)) {
				logger.debug("Corrupt URL for BenchmarkAppConnector.benchmarkAppDirectory Property, correcting ...");
				BenchmarkAppConnector.benchmarkAppDirectory += File.separator;
			}
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			throw new Exception(ex.getMessage());
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
					throw new Exception(e.getMessage());
				}
			}
			// Marking for GC
			input = null;
			prop = null;
		}

	}
	
	/**
	 * A method to archive the data files.
	 * @throws Exception 
	 */
	public static void archive() throws Exception {
		
		logger.debug(" Archiving data ...");
		File sourceDataFolder = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data");

		if (sourceDataFolder.exists() && sourceDataFolder.listFiles().length != 0) {

			Date dNow = new Date();
			SimpleDateFormat ft = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");		
			
			File archDirectory = new File(Util.TEST_DATA_ARCHIVE_LOCATION + "archive-" + ft.format(dNow));
			if (!archDirectory.exists()) {
				archDirectory.mkdirs();
			}

			Util.copyFolder(sourceDataFolder, archDirectory);
			Util.execute("rm -r -f " + sourceDataFolder.getAbsolutePath(), sourceDataFolder.getAbsolutePath());

		} else {
			logger.debug(" No files to archive ...");
		}
		
		logger.debug(" Archiving data process [COMPLETE] ...");
	}
	
	/**
	 * A method to clean the data director in the webapp.
	 * @throws Exception 
	 */
	public static void clearData() throws Exception {
		
		logger.debug(" Clearing data ...");
		
		File sourceDataFolder = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data");
		Util.execute("rm -r -f " + sourceDataFolder.getAbsolutePath(), sourceDataFolder.getAbsolutePath());

		logger.debug(" Clearing data [COMPLETE] ...");
	}

	/**
	 * A method to check if all the data files are present.
	 * @return boolean
	 * @throws Exception 
	 */
	public static boolean checkDataFiles() throws Exception {

		logger.debug(" Checking if data files are present ...");

		File file = new File(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");
			
			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.ONEM_TEST_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.ONEM_TEST_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.ONEM_TEST_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_TERM_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_TERM_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.NUMERIC_QUERY_TERM_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_TERM_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.NUMERIC_QUERY_TERM_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.NUMERIC_QUERY_TERM_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_PAIR_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_PAIR_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.NUMERIC_QUERY_PAIR_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_PAIR_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.NUMERIC_QUERY_PAIR_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.NUMERIC_QUERY_PAIR_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_SORTED_QUERY_PAIR_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.NUMERIC_SORTED_QUERY_PAIR_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.NUMERIC_SORTED_QUERY_PAIR_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_SORTED_QUERY_PAIR_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.NUMERIC_SORTED_QUERY_PAIR_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.NUMERIC_SORTED_QUERY_PAIR_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_AND_OR_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_AND_OR_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.NUMERIC_QUERY_AND_OR_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_AND_OR_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.NUMERIC_QUERY_AND_OR_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.NUMERIC_QUERY_AND_OR_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.TEXT_TERM_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.TEXT_TERM_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.TEXT_TERM_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.TEXT_TERM_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.TEXT_TERM_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.TEXT_TERM_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.TEXT_PHRASE_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.TEXT_PHRASE_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.TEXT_PHRASE_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.TEXT_PHRASE_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.TEXT_PHRASE_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.TEXT_PHRASE_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.HIGHLIGHT_TERM_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.HIGHLIGHT_TERM_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.HIGHLIGHT_TERM_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.HIGHLIGHT_TERM_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.HIGHLIGHT_TERM_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.HIGHLIGHT_TERM_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		file = new File(Util.TEST_DATA_DIRECTORY + Util.RANGE_FACET_DATA);
		if (!file.exists()) {
			logger.debug(" Data File: " + file.getName() + " is not present. Trying to download ...");

			Util.execute("wget -O " + Util.TEST_DATA_DIRECTORY + Util.RANGE_FACET_DATA + " " + Util.TEST_DATA_STORE_LOCATION + Util.RANGE_FACET_DATA, Util.TEST_DATA_DIRECTORY);

			File downloadedfile = new File(Util.TEST_DATA_DIRECTORY + Util.RANGE_FACET_DATA);
			if (downloadedfile.exists()) {
				logger.debug("File " + Util.RANGE_FACET_DATA + " is now downloaded ...");
			} else {
				logger.debug("Error Downloading File " + Util.RANGE_FACET_DATA + " Please try manually or check if location has changed ...");
				return false;
			}
		}

		logger.debug(" All Required Data Files are present ...");
		return true;
	}

	/**
	 * A method used for sending requests to web resources.
	 * 
	 * @param url
	 * @param type
	 * @return
	 * @throws Exception 
	 */
	public static String getResponse(String url, String type) throws Exception {

		Client client;
		ClientResponse response;

		try {
			client = Client.create();
			WebResource webResource = client.resource(url);
			response = webResource.accept(type).get(ClientResponse.class);

			if (response.getStatus() != 200) {
				logger.error("Failed : HTTP error code : " + response.getStatus());
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}

			return response.getEntity(String.class);
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			// Marking for GC
			client = null;
			response = null;
		}
	}

	/**
	 * A method used for getting information from the metric API.
	 * 
	 * @param commitID
	 * @param port
	 * @throws Exception 
	 */
	public static void getEnvironmentInformationFromMetricAPI(String commitID, String port) throws Exception {

		String response = Util.getResponse("http://localhost:" + port + "/solr/admin/metrics?wt=json&group=jvm",
				MediaType.APPLICATION_JSON);
		JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

		String printString = "<table class='table table-striped'><thead><tr><th>Metric</th><th>Details</th></tr></thead><tbody>";
		printString += "<tr><td>Memory Heap Committed:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.committed")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Memory Heap Init:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.init")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Memory Heap Max:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.max")
				+ " Bytes</td></tr>\n";

		printString += "<tr><td>Memory Non-Heap Committed:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm"))
						.get("memory.non-heap.committed")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Memory Non-Heap Init:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.non-heap.init")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Memory Non-Heap Max:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.non-heap.max")
				+ " Bytes</td></tr>\n";

		printString += "<tr><td>Memory Total Committed:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.total.committed")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Memory Total Init:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.total.init")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Memory Total Max:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("memory.total.max")
				+ " Bytes</td></tr>\n";

		printString += "<tr><td>Total Physical Memory:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm"))
						.get("os.totalPhysicalMemorySize")
				+ " Bytes</td></tr>\n";
		printString += "<tr><td>Total Swap Space:</td><td>"
				+ ((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm")).get("os.totalSwapSpaceSize")
				+ " Bytes</td></tr>\n";

		printString += "</tbody></table>";

		BenchmarkAppConnector.writeToWebAppDataFile(
				Util.TEST_ID + "_" + commitID + "_" + FileType.TEST_ENV_FILE + "_dump.csv", printString, true,
				FileType.TEST_ENV_FILE);

		response = null;
		jsonObject = null;
		printString = null;
	}

	/**
	 * A method that checks if the webapp files are present. If not, this method
	 * copies the required files into the required directory.
	 * @throws Exception 
	 */
	public static void checkWebAppFiles() throws Exception {

		logger.debug(" Verifying that the Webapp files are present ... ");

		File webAppSourceDir = new File("src/main/webapp");
		File webAppTargetDir = new File(BenchmarkAppConnector.benchmarkAppDirectory);

		if (!webAppTargetDir.exists()) {
			webAppTargetDir.mkdir();
		}

		try {

			if (!webAppTargetDir.exists()) {
				logger.debug(" Webapp target directory not present creating now! ... ");
				webAppTargetDir.mkdir();

				if (!new File(
						BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST")
								.exists()) {
					logger.debug(" Copying updated/new webapp files ...");
					Util.copyFolder(webAppSourceDir, webAppTargetDir);
				}

				File flagFile = new File(
						BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST");
				flagFile.createNewFile();

			} else if (!new File(
					BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST")
							.exists()) {
				logger.debug(" Copying updated/new webapp files ...");
				Util.copyFolder(webAppSourceDir, webAppTargetDir);

				File flagFile = new File(
						BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST");
				flagFile.createNewFile();

			} else {
				logger.debug(" Webapp files seems present, skipping copying webapp files ...");
			}

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			webAppSourceDir = null;
			webAppTargetDir = null;
		}

	}

	/**
	 * A method used for copying contents from one folder to the other.
	 * 
	 * @param source
	 * @param destination
	 * @throws Exception 
	 */
	public static void copyFolder(File source, File destination) throws Exception {
		
		logger.debug("Copying: " + source);
		
		if (source.isDirectory()) {
			if (!destination.exists()) {
				destination.mkdirs();
			}

			String files[] = source.list();

			for (String file : files) {
				File srcFile = new File(source, file);
				File destFile = new File(destination, file);

				copyFolder(srcFile, destFile);
			}
		} else {
			InputStream in = null;
			OutputStream out = null;

			try {
				in = new FileInputStream(source);
				out = new FileOutputStream(destination);

				byte[] buffer = new byte[1024];

				int length;
				while ((length = in.read(buffer)) > 0) {
					out.write(buffer, 0, length);
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			} finally {
				try {
					in.close();
					out.close();
				} catch (IOException e1) {
					logger.error(e1.getMessage());
					throw new Exception(e1.getMessage());
				}
			}
		}
	}

	/**
	 * A method used for setting up the alive flag file.
	 * 
	 * @throws IOException
	 */
	public static void setAliveFlag() throws IOException {

		File statusFile = new File(BenchmarkAppConnector.benchmarkAppDirectory + "iamalive.txt");

		if (!statusFile.exists()) {
			statusFile.createNewFile();
		}
		// Marking for GC
		statusFile = null;
	}

	/**
	 * A method used for setting up the dead flag file.
	 */
	public static void setDeadFlag() {

		File statusFile = new File(BenchmarkAppConnector.benchmarkAppDirectory + "iamalive.txt");
		if (statusFile.exists()) {
			statusFile.delete();
		}
		// Marking for GC
		statusFile = null;
	}

	/**
	 * A method used for capturing the command line args for this package.
	 * 
	 * @param args
	 * @return
	 */
	public static List<String> getArgs(String[] args) {

		List<String> argsList = new LinkedList<String>();
		for (int i = 0; i < args.length; i++) {
			argsList.add(args[i]);
		}
		return argsList;
	}

	/**
	 * A method used for initializing and executing the benchmarks.
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void init(String[] args) {

		logger.info("--------------------------------------------------------------------");
		logger.info("          	*** Solr Nightly Benchmarks ***  HOLA !!!                ");
		logger.info("--------------------------------------------------------------------");
		
		try {
			Util.getPropertyValues();

			if (new File(Util.DOWNLOAD_DIR + "git-repository/solr/package/").exists()) {
				Util.execute("rm -r -f " + Util.DOWNLOAD_DIR + "git-repository/solr/package/", Util.DOWNLOAD_DIR);
			}
			
			argsList = Util.getArgs(args);

			if (argsList.size() == 0) {
				logger.debug(" No Parameters defined! [EXITING] ...");
				logger.debug(
						" Please access: https://github.com/viveknarang/lucene-solr/tree/SolrNightlyBenchmarks/dev-tools/SolrNightBenchmarks#possible-parameters ...\n\n");
				System.exit(0);
			} else {

				for (int i = 0; i < argsList.size(); i++) {
					if (argsList.get(i).equals("--clean-up") 
							|| argsList.get(i).equals("--commit-id")
							|| argsList.get(i).equals("--test-with-number-of-documents")
							|| argsList.get(i).equals("--latest-commit") 
							|| argsList.get(i).equals("--archive")
							|| argsList.get(i).equals("--clear-data")
							|| argsList.get(i).equals("--use-sample-dataset")) {
					} else {
						if (argsList.get(i).startsWith("--")) {
							logger.debug("");
							logger.info(argsList.get(i) + " seems like an unrecognized argument...");
						}
					}
				}
				logger.debug("");

				int atleastOne = 0;
				
				if (argsList.contains("--clear-data")) {
					atleastOne++;
				}
				if (argsList.contains("--use-sample-dataset")) {
					atleastOne++;
				}
				if (argsList.contains("--archive")) {
					atleastOne++;
				}
				if (argsList.contains("--test-with-number-of-documents")) {

					try {
						Long.parseLong(argsList.get(argsList.indexOf("--test-with-number-of-documents") + 1));
						atleastOne++;
					} catch (Exception e) {
						logger.debug(
								" Parameter value for --test-with-number-of-documents should be a number! [EXITING] ...\n\n");
						System.exit(0);
					}

				}
				if (argsList.contains("--latest-commit")) {
					atleastOne++;
				}
				if (argsList.contains("--commit-id")) {

					try {
						argsList.get(argsList.indexOf("--commit-id") + 1);
						atleastOne++;
					} catch (Exception e) {
						logger.debug(" Parameter value for --commit-id not defined! [EXITING] ...");
						logger.debug(
								" Please access: https://github.com/viveknarang/lucene-solr/tree/SolrNightlyBenchmarks/dev-tools/SolrNightBenchmarks#possible-parameters ...\n\n");
						System.exit(0);
					}

				}

				if (atleastOne == 0) {
					logger.debug(" No Valid Parameters defined! [EXITING] ...");
					logger.debug(
							" Please access: https://github.com/viveknarang/lucene-solr/tree/SolrNightlyBenchmarks/dev-tools/SolrNightBenchmarks#possible-parameters ...\n\n");
					System.exit(0);
				}

			}

			File datafile = new File(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA);
			if (!datafile.exists()) {
				logger.debug(" Data File " + Util.ONEM_TEST_DATA + " Missing! [EXITING] ...\n\n");
				System.exit(0);
			}
			datafile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_TERM_DATA);
			if (!datafile.exists()) {
				logger.debug(" Data File " + Util.NUMERIC_QUERY_TERM_DATA + " Missing! [EXITING] ...\n\n");
				System.exit(0);
			}
			datafile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_PAIR_DATA);
			if (!datafile.exists()) {
				logger.debug(" Data File " + Util.NUMERIC_QUERY_PAIR_DATA + " Missing! [EXITING] ...\n\n");
				System.exit(0);
			}
			datafile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_SORTED_QUERY_PAIR_DATA);
			if (!datafile.exists()) {
				logger.debug(" Data File " + Util.NUMERIC_SORTED_QUERY_PAIR_DATA + " Missing! [EXITING] ...\n\n");
				System.exit(0);
			}
			
			if (argsList.contains("--clean-up")) {
				logger.debug("Initiating Housekeeping activities! ... ");
				
				File dir = new File(Util.DOWNLOAD_DIR);
				if (dir != null && dir.isDirectory()) {
					File[] files = dir.listFiles();
					for (File f : files) {
							if (f.getName().contains("git-repository")) {
								continue;		
							} else if (f.getName().contains("zookeeper")) {
								continue;
							} else {
								f.delete();
							}
					}
 				}
			}
			
			if (argsList.contains("--archive")) {
				Util.archive();
				System.exit(0);
			}

			if (argsList.contains("--clear-data")) {
				Util.clearData();
				System.exit(0);
			}

			if (argsList.contains("--test-with-number-of-documents")) {
				long numDocuments = Long
						.parseLong(argsList.get(argsList.indexOf("--test-with-number-of-documents") + 1));

				if (numDocuments > 0 && numDocuments <= 1000000) {
					Util.TEST_WITH_NUMBER_OF_DOCUMENTS = numDocuments;
					logger.debug(" Number of Documents to test with: " + Util.TEST_WITH_NUMBER_OF_DOCUMENTS);
				}
			}

			if (argsList.contains("--silent")) {
				logger.debug(" Running silently since --silent parameter is set ...");
				Util.SILENT = true;
			}

			Util.checkBaseAndTempDir();

			Util.setAliveFlag();

			Util.getSystemEnvironmentInformation();

			if (!BenchmarkAppConnector.isRunningFolderEmpty()) {
				logger.debug(" It looks like the last test session failed or was aborted ...");

				Util.killProcesses("zookeeper");
				Util.killProcesses("Dsolr.jetty.https.port");

				if (!BenchmarkAppConnector.isCloningFolderEmpty()) {
					logger.debug(" Looks like a broken clone exists removing it ...");
					Util.execute("rm -r -f " + Util.getLocalRepoPath(), Util.getLocalRepoPath());
				}

				Thread.sleep(5000);

				Util.cleanRunDirectory();
				Util.deleteRunningFile();
			}
			
			if (argsList.contains("--use-sample-dataset")) {
				
				String load;
				try {
					 load = argsList.get(argsList.indexOf("--use-sample-dataset") + 1); // load from 0 to 1
				} catch (Exception e) {
					throw new Exception("Value for --use-sample-dataset is not defined!");
				}
				
				if (Double.parseDouble(load) > 1.0 || Double.parseDouble(load) < 0) {
					throw new Exception("--use-sample-dataset not in range: please set value between 0 < x <= 1");
				}
				
				if (argsList.contains("--latest-commit")) {
					Util.createIsRunningFile();
					Util.COMMIT_ID = Util.getLatestCommitID(Util.LUCENE_SOLR_REPOSITORY_URL);
					logger.debug("The latest commit ID is: " + Util.COMMIT_ID);
					TestPlans.sampleTest(Double.parseDouble(load));
					BenchmarkAppConnector.publishDataForWebApp();
					BenchmarkReportData.reset();
					if (new File(Util.DOWNLOAD_DIR + "git-repository/solr/package/").exists()) {
						Util.execute("rm -r -f " + Util.DOWNLOAD_DIR + "git-repository/solr/package/", Util.DOWNLOAD_DIR);
					}
				} else if (argsList.contains("--commit-id")) {

					try {
						Util.COMMIT_ID = argsList.get(argsList.indexOf("--commit-id") + 1);
					} catch (Exception e) {
						throw new Exception("Value for --commit-id is not defined!");
					}
					
					Util.createIsRunningFile();
					logger.debug(" Executing benchmarks with commit: " + Util.COMMIT_ID);
					TestPlans.sampleTest(Double.parseDouble(load));
					BenchmarkAppConnector.publishDataForWebApp();
					BenchmarkReportData.reset();
					if (new File(Util.DOWNLOAD_DIR + "git-repository/solr/package/").exists()) {
						Util.execute("rm -r -f " + Util.DOWNLOAD_DIR + "git-repository/solr/package/", Util.DOWNLOAD_DIR);
					}
				}
			} else {

				if (argsList.contains("--latest-commit")) {
					Util.createIsRunningFile();
					Util.COMMIT_ID = Util.getLatestCommitID(Util.LUCENE_SOLR_REPOSITORY_URL);
					logger.debug("The latest commit ID is: " + Util.COMMIT_ID);
	
					TestPlans.execute();
					BenchmarkAppConnector.publishDataForWebApp();
					BenchmarkReportData.reset();
					if (new File(Util.DOWNLOAD_DIR + "git-repository/solr/package/").exists()) {
						Util.execute("rm -r -f " + Util.DOWNLOAD_DIR + "git-repository/solr/package/", Util.DOWNLOAD_DIR);
					}
				} else if (argsList.contains("--commit-id")) {
					Util.createIsRunningFile();
					Util.COMMIT_ID = argsList.get(argsList.indexOf("--commit-id") + 1);
					logger.debug(" Executing benchmarks with commit: " + Util.COMMIT_ID);
					TestPlans.execute();
					BenchmarkAppConnector.publishDataForWebApp();
					BenchmarkReportData.reset();
					if (new File(Util.DOWNLOAD_DIR + "git-repository/solr/package/").exists()) {
						Util.execute("rm -r -f " + Util.DOWNLOAD_DIR + "git-repository/solr/package/", Util.DOWNLOAD_DIR);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	/**
	 * A method used for doing the cleanup after the benchmarks cycle end.
	 */
	public static void destroy() {

		try {
			Util.setDeadFlag();
			Util.createLastRunFile();
			Util.deleteRunningFile();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	/**
	 * A method used for creating the last run file.
	 * @throws Exception 
	 */
	public static void createLastRunFile() throws Exception {

		BenchmarkAppConnector.deleteFolder(FileType.LAST_RUN_COMMIT);
		BenchmarkAppConnector.writeToWebAppDataFile(Util.COMMIT_ID, "", true, FileType.LAST_RUN_COMMIT);

	}

	/**
	 * A method used for creating the running flag file.
	 * @throws Exception 
	 */
	public static void createIsRunningFile() throws Exception {

		BenchmarkAppConnector.deleteFolder(FileType.IS_RUNNING_FILE);
		BenchmarkAppConnector.writeToWebAppDataFile(Util.COMMIT_ID, "", true, FileType.IS_RUNNING_FILE);

	}

	/**
	 * A method used for deleting the running file.
	 * @throws Exception 
	 */
	public static void deleteRunningFile() throws Exception {
		BenchmarkAppConnector.deleteFolder(FileType.IS_RUNNING_FILE);
	}

	/**
	 * A method used for cleaning the run directory.
	 * @throws Exception 
	 */
	public static void cleanRunDirectory() throws Exception {
		Util.execute("rm -r -f " + Util.RUN_DIR, Util.RUN_DIR);
	}

	/**
	 * A method used for generating random sentences for tests.
	 * 
	 * @param r
	 * @param words
	 * @return String
	 */
	public static String getSentence(Random r, int words) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < words; i++) {
			sb.append(TestUtil.randomSimpleString(r, 4 + r.nextInt(10)) + " ");
		}
		return sb.toString().trim();
	}

	/**
	 * A method used for creating a test data file.
	 * 
	 * @param fileName
	 * @param numberOfDocuments
	 * @throws Exception 
	 */
	public static void createTestDataFile(String fileName, int numberOfDocuments) throws Exception {
		logger.debug(" Preparing 4k text documents");
		for (int i = 0; i < numberOfDocuments; i++) {
			if (i % 100 == 0) {
				logger.debug("|");
			}

			Random r = new Random();

			String line = i + "," + getSentence(r, 400) + "," + (new Random().nextInt()) + ","
					+ (new Random().nextLong()) + "," + "Category" + (new Random().nextInt(10)) + ","
					+ getSentence(r, 350);
			BenchmarkAppConnector.writeToWebAppDataFile(fileName, line, false, FileType.TEST_ENV_FILE);
		}
	}

	/**
	 * A method used for creating numeric sorting query data file.
	 * 
	 * @param fileName
	 * @param numberOfDocuments
	 * @throws Exception 
	 */
	public static void createNumericSortedQueryDataFile(String fileName, int numberOfDocuments) throws Exception {
		logger.debug(" Preparing sorted numeric query data ...");
		for (int i = 0; i < numberOfDocuments; i++) {
			if (i % 100 == 0) {
				logger.debug("|");
			}

			Random r = new Random();
			int number = r.nextInt((1000000 - 100));

			String line = number + "," + (number + 100);

			BenchmarkAppConnector.writeToWebAppDataFile(fileName, line, false, FileType.TEST_ENV_FILE);
		}
		logger.debug(" Preparation [COMPLETE] ...");
	}

	/**
	 * A method used for locating and killing unused processes.
	 * 
	 * @param lookFor
	 * @throws IOException 
	 */
	public static void killProcesses(String lookFor) throws IOException {

		logger.debug(" Searching and killing " + lookFor + " process(es) ...");

		BufferedReader reader;
		String line = "";

		try {
			String[] cmd = { "/bin/sh", "-c", "ps -ef | grep " + lookFor + " | awk '{print $2}'" };
			reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(cmd).getInputStream()));

			while ((line = reader.readLine()) != null) {

				line = line.trim();
				logger.debug(" Found " + lookFor + " Running with PID " + line + " Killing now ..");
				Runtime.getRuntime().exec("kill -9 " + line);
			}

			reader.close();

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		} finally {
			// Marking for GC
			reader = null;
			line = null;
		}

	}

	/**
	 * A utility method used for creating the Highlight keywords.
	 * @throws Exception 
	 */
	public static void createHighlightKeywordsDataFile() throws Exception {

		String line = "";
		String cvsSplitBy = ",";

		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {
				String[] data = line.split(cvsSplitBy);
				Random r = new Random();
				int number = r.nextInt(data[2].split(" ").length);
				String s = data[2].split(" ")[number].trim();

				while (s.length() < 10) {
					s = data[2].split(" ")[r.nextInt(data[2].split(" ").length)].trim();
				}

				logger.debug(data[2].split(" ")[number]);
				BenchmarkAppConnector.writeToWebAppDataFile("Highlight-Terms.csv", s, false, FileType.TEST_ENV_FILE);
			}

		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}

	}

	/**
	 * A Method used for creating ranges for facet tests.
	 * @throws Exception 
	 */
	public static void createRangeFacetDataFile() throws Exception {

		String line = "";
		String cvsSplitBy = ",";
		long number = 0;
		
		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			Set<Long> set = new TreeSet<Long>();
			
			while ((line = br.readLine()) != null) {
				String[] data = line.split(cvsSplitBy);

				String s = data[5].trim();
				set.add(Long.parseLong(data[5].trim()));

				logger.debug("Adding to Set: " + s);
				number++;
			}

			List<Long> list = new LinkedList<Long>();

			for (Long l: set) {
				logger.debug("" + l);
				list.add(l);
			}
			
			for (int i = 0; i < (number/2); i++) {
				
				int l = new Random().nextInt(list.size());
				int m = 150000 + new Random().nextInt(list.size()/2);
				
				String s = "";
				
				if (list.get(l) < list.get(m)) {
					s = list.get(l) + "," + list.get(m) + ",1000000";
				} else {
					s = list.get(m) + "," + list.get(l) + ",1000000";
				}
					
				logger.debug(s);
				BenchmarkAppConnector.writeToWebAppDataFile("Range-Facet-Test-Ranges.csv", s, false, FileType.TEST_ENV_FILE);
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}

	}

	/**
	 * A method used for creating test data file using Wikipedia data.
	 * @throws Exception 
	 */
	public static void CreateWikiDataFile() throws Exception {

		try {

			int id = 1;

			for (int i = 1; i <= 5; i++) {

				File fXmlFile = new File("/home/vivek/data/enwiki-20170520-pages-articles" + i + ".xml");
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fXmlFile);

				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("page");

				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);

					if (nNode.getNodeType() == Node.ELEMENT_NODE) {

						Element eElement = (Element) nNode;
						String data = (id++) + ","
								+ eElement.getElementsByTagName("title").item(0).getTextContent().replaceAll("\n", "")
										.replaceAll("\t", "").replaceAll("\r", "").replaceAll("( )+", " ")
										.replaceAll("[^\\sa-zA-Z0-9]", "").replaceAll("[a-zA-Z0-9]{30,}", "").trim()
								+ ","
								+ eElement.getElementsByTagName("revision").item(0).getTextContent()
										.replaceAll("\n", "").replaceAll("\t", "").replaceAll("\r", "")
										.replaceAll("( )+", " ").replaceAll("[^\\sa-zA-Z0-9]", "")
										.replaceAll("[a-zA-Z0-9]{30,}", "").trim()
								+ "," + "Category-" + new Random().nextInt(10) + "," + new Random().nextInt() + ","
								+ new Random().nextInt() + "," + new Random().nextFloat() + ","
								+ new Random().nextLong() + "," + new Random().nextDouble() + ","
								+ RandomStringUtils.randomAlphabetic(20);

						BenchmarkAppConnector.writeToWebAppDataFile("en-wiki-data-2G-modified.csv", data, false,
								FileType.TEST_ENV_FILE);

						logger.debug("\r" + id);
					}
				}

			}

		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * A utility method used for getting the head name.
	 * @param repo
	 * @return string
	 * @throws Exception 
	 */
	public static String getHeadName(Repository repo) throws Exception {
		  String result = null;
		  try {
		    ObjectId id = repo.resolve(Constants.HEAD);
		    result = id.getName();
		  } catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		  }
		  return result;
	}
}