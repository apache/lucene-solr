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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
 * 
 * @author Vivek Narang
 *
 */
public class Util {

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
	public static String COMMIT_ID;
	public static String TEST_ID = UUID.randomUUID().toString();
	public static String TEST_TIME = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());
	public static String METRIC_ESTIMATION_PERIOD = "1000";
	public static String QUERY_THREAD_COUNT = "1";
	public static String TEST_DATA_DIRECTORY = "";
	public static String ONEM_TEST_DATA = "";
	public static String NUMERIC_QUERY_TERM_DATA = "";
	public static String NUMERIC_QUERY_PAIR_DATA = "";
	public static String NUMERIC_QUERY_AND_OR_DATA = "";
	public static String NUMERIC_SORTED_QUERY_PAIR_DATA = "";
	public static String TEXT_TERM_DATA = "";
	public static String TEXT_PHRASE_DATA = "";

	public static long TEST_WITH_NUMBER_OF_DOCUMENTS = 100000;
	public static boolean USE_COLORED_TEXT_ON_CONSOLE = true;

	public static boolean SILENT = false;

	final static Logger logger = Logger.getLogger(Util.class);

	static List<String> argsList;

	/**
	 * A method used for wrapping up the output from the system (on console or
	 * log).
	 * 
	 * @param message
	 * @param type
	 * @param printInLog
	 */
	public static void postMessage(String message, MessageType type, boolean printInLog) {

		String ANSI_RESET = "\u001B[0m";
		String ANSI_BLACK = "\u001B[30m";
		String ANSI_RED = "\u001B[31m";
		String ANSI_GREEN = "\u001B[32m";
		String ANSI_YELLOW = "\u001B[33m";
		String ANSI_BLUE = "\u001B[34m";
		String ANSI_PURPLE = "\u001B[35m";
		String ANSI_CYAN = "\u001B[36m";
		String ANSI_WHITE = "\u001B[37m";

		if (!SILENT && USE_COLORED_TEXT_ON_CONSOLE) {
			if (type.equals(MessageType.WHITE_TEXT)) {
				System.out.println(ANSI_WHITE + message + ANSI_RESET);
			} else if (type.equals(MessageType.BLUE_TEXT)) {
				System.out.println(ANSI_BLUE + message + ANSI_RESET);
			} else if (type.equals(MessageType.YELLOW_TEXT)) {
				System.out.println(ANSI_YELLOW + message + ANSI_RESET);
			} else if (type.equals(MessageType.RED_TEXT)) {
				System.out.println(ANSI_RED + message + ANSI_RESET);
			} else if (type.equals(MessageType.GREEN_TEXT)) {
				System.out.println(ANSI_GREEN + message + ANSI_RESET);
			} else if (type.equals(MessageType.BLACK_TEXT)) {
				System.out.println(ANSI_BLACK + message + ANSI_RESET);
			} else if (type.equals(MessageType.PURPLE_TEXT)) {
				System.out.println(ANSI_PURPLE + message + ANSI_RESET);
			} else if (type.equals(MessageType.CYAN_TEXT)) {
				System.out.println(ANSI_CYAN + message + ANSI_RESET);
			}
		} else {
			System.out.println(message);
		}

		if (printInLog) {
			logger.info(message);
		}

	}

	/**
	 * A method used for invoking a process with specific parameters.
	 * 
	 * @param command
	 * @param workingDirectoryPath
	 * @return
	 */
	public static int execute(String command, String workingDirectoryPath) {
		Util.postMessage("Executing: " + command, MessageType.WHITE_TEXT, true);
		Util.postMessage("Working dir: " + workingDirectoryPath, MessageType.WHITE_TEXT, true);
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
			Util.postMessage(e.getMessage(), MessageType.RED_TEXT, true);
			return -1;
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
		baseDirectory.mkdir();
		File tempDirectory = new File(DOWNLOAD_DIR);
		tempDirectory.mkdir();

		// Marking for GC
		baseDirectory = null;
		tempDirectory = null;
	}

	/**
	 * A method used for get an available free port for running the
	 * solr/zookeeper node on.
	 * 
	 * @return int
	 */
	public static int getFreePort() {

		int port = ThreadLocalRandom.current().nextInt(10000, 60000);
		Util.postMessage("Looking for a free port ... Checking availability of port number: " + port,
				MessageType.WHITE_TEXT, true);
		ServerSocket serverSocket = null;
		DatagramSocket datagramSocket = null;
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);
			datagramSocket = new DatagramSocket(port);
			datagramSocket.setReuseAddress(true);
			Util.postMessage("Port " + port + " is free to use. Using this port !!", MessageType.GREEN_TEXT, true);
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
				}
			}
			// Marking for GC
			serverSocket = null;
			datagramSocket = null;
		}

		Util.postMessage("Port " + port + " looks occupied trying another port number ... ", MessageType.RED_TEXT,
				true);
		return getFreePort();
	}

	/**
	 * 
	 * @param plaintext
	 * @return String
	 */
	static public String md5(String plaintext) {
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
			e.printStackTrace();
		}
		return hashtext;
	}

	/**
	 * A metod used for extracting files from an archive.
	 * 
	 * @param zipIn
	 * @param filePath
	 * @throws IOException
	 */
	public static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {

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

			Util.postMessage(e.getMessage(), MessageType.RED_TEXT, true);

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
	 */
	public static void download(String downloadURL, String fileDownloadLocation) {

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
				Util.postMessageOnLine("\r" + size + " ");
				fos.write(buf, 0, n);
			}
			fos.close();
			in.close();

		} catch (Exception e) {

			Util.postMessage(e.getMessage(), MessageType.RED_TEXT, false);

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
		Util.postMessage("** Attempting to unzip the downloaded release ...", MessageType.WHITE_TEXT, true);
		try {
			ZipFile zip = new ZipFile(filename);
			zip.extractAll(filePath);
		} catch (ZipException ex) {
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
		Util.postMessage("** Getting the latest commit ID from: " + repositoryURL, MessageType.BLUE_TEXT, false);
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
		Util.GIT_REPOSITORY_PATH = Util.DOWNLOAD_DIR + "git-repository-" + Util.COMMIT_ID;
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
	 */
	public static void getAndPublishCommitInformation() {
		BenchmarkAppConnector.writeToWebAppDataFile(
				Util.TEST_ID + "_" + Util.COMMIT_ID + "_COMMIT_INFORMATION_dump.csv", Util.getCommitInformation(), true,
				FileType.COMMIT_INFORMATION_FILE);
	}

	/**
	 * A method used for getting the commit information.
	 * 
	 * @return String
	 */
	public static String getCommitInformation() {
		Util.postMessage("** Getting the latest commit Information from local repository", MessageType.BLUE_TEXT,
				false);
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
			e.printStackTrace();
			return "";
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
	 */
	public static void getSystemEnvironmentInformation() {

		Util.postMessage("** Getting the test environment information", MessageType.BLUE_TEXT, false);

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
			e.printStackTrace();
		} finally {
			reader = null;
			returnString = null;
			line = null;
		}
	}

	/**
	 * A method used for reading the property file and injecting data into the
	 * data variables.
	 */
	public static void getPropertyValues() {

		// THIS METHOD SHOULD BE CALLED BEFORE ANYOTHER METHOD

		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("config.properties");
			prop.load(input);

			// get the property value and print it out
			BenchmarkAppConnector.benchmarkAppDirectory = prop
					.getProperty("SolrNightlyBenchmarks.benchmarkAppDirectory");
			Util.postMessage(
					"Getting Property Value for benchmarkAppDirectory: " + BenchmarkAppConnector.benchmarkAppDirectory,
					MessageType.YELLOW_TEXT, false);
			SolrIndexingClient.solrCommitHistoryData = prop.getProperty("SolrNightlyBenchmarks.solrCommitHistoryData");
			Util.postMessage(
					"Getting Property Value for solrCommitHistoryData: " + SolrIndexingClient.solrCommitHistoryData,
					MessageType.YELLOW_TEXT, false);
			SolrIndexingClient.amazonFoodData = prop.getProperty("SolrNightlyBenchmarks.amazonFoodData");
			Util.postMessage("Getting Property Value for amazonFoodData: " + SolrIndexingClient.amazonFoodData,
					MessageType.YELLOW_TEXT, false);
			MetricCollector.metricsURL = prop.getProperty("SolrNightlyBenchmarks.metricsURL");
			Util.postMessage("Getting Property Value for metricsURL: " + MetricCollector.metricsURL,
					MessageType.YELLOW_TEXT, false);
			Util.ZOOKEEPER_DOWNLOAD_URL = prop.getProperty("SolrNightlyBenchmarks.zookeeperDownloadURL");
			Util.postMessage("Getting Property Value for zookeeperDownloadURL: " + Util.ZOOKEEPER_DOWNLOAD_URL,
					MessageType.YELLOW_TEXT, false);
			Util.ZOOKEEPER_RELEASE = prop.getProperty("SolrNightlyBenchmarks.zookeeperDownloadVersion");
			Util.postMessage("Getting Property Value for zookeeperDownloadVersion: " + Util.ZOOKEEPER_RELEASE,
					MessageType.YELLOW_TEXT, false);
			Util.ZOOKEEPER_IP = prop.getProperty("SolrNightlyBenchmarks.zookeeperHostIp");
			Util.postMessage("Getting Property Value for zookeeperHostIp: " + Util.ZOOKEEPER_IP,
					MessageType.YELLOW_TEXT, false);
			Util.ZOOKEEPER_PORT = prop.getProperty("SolrNightlyBenchmarks.zookeeperHostPort");
			Util.postMessage("Getting Property Value for zookeeperHostPort: " + Util.ZOOKEEPER_PORT,
					MessageType.YELLOW_TEXT, false);
			Util.LUCENE_SOLR_REPOSITORY_URL = prop.getProperty("SolrNightlyBenchmarks.luceneSolrRepositoryURL");
			Util.postMessage("Getting Property Value for luceneSolrRepositoryURL: " + Util.LUCENE_SOLR_REPOSITORY_URL,
					MessageType.YELLOW_TEXT, false);
			Util.METRIC_ESTIMATION_PERIOD = prop.getProperty("SolrNightlyBenchmarks.metricEstimationPeriod");
			Util.postMessage("Getting Property Value for metricEstimationPeriod: " + Util.METRIC_ESTIMATION_PERIOD,
					MessageType.YELLOW_TEXT, false);
			Util.QUERY_THREAD_COUNT = prop.getProperty("SolrNightlyBenchmarks.queryThreadCount");
			Util.postMessage("Getting Property Value for queryThreadCount: " + Util.QUERY_THREAD_COUNT,
					MessageType.YELLOW_TEXT, false);
			Util.TEST_DATA_DIRECTORY = prop.getProperty("SolrNightlyBenchmarks.testDataDirectory");
			Util.postMessage("Getting Property Value for testDataDirectory: " + Util.TEST_DATA_DIRECTORY,
					MessageType.YELLOW_TEXT, false);
			Util.ONEM_TEST_DATA = prop.getProperty("SolrNightlyBenchmarks.1MTestData");
			Util.postMessage("Getting Property Value for 1MTestData: " + Util.ONEM_TEST_DATA, MessageType.YELLOW_TEXT,
					false);
			Util.NUMERIC_QUERY_TERM_DATA = prop.getProperty("SolrNightlyBenchmarks.staticNumericQueryTermsData");
			Util.postMessage("Getting Property Value for staticNumericQueryTermsData: " + Util.NUMERIC_QUERY_TERM_DATA,
					MessageType.YELLOW_TEXT, false);
			Util.NUMERIC_QUERY_PAIR_DATA = prop.getProperty("SolrNightlyBenchmarks.staticNumericQueryPairsData");
			Util.postMessage("Getting Property Value for staticNumericQueryPairsData: " + Util.NUMERIC_QUERY_PAIR_DATA,
					MessageType.YELLOW_TEXT, false);
			Util.TEST_WITH_NUMBER_OF_DOCUMENTS = Long
					.parseLong(prop.getProperty("SolrNightlyBenchmarks.testWithNumberOfDocuments"));
			Util.postMessage(
					"Getting Property Value for testWithNumberOfDocuments: " + Util.TEST_WITH_NUMBER_OF_DOCUMENTS,
					MessageType.YELLOW_TEXT, false);
			Util.NUMERIC_SORTED_QUERY_PAIR_DATA = prop
					.getProperty("SolrNightlyBenchmarks.staticNumericSortedQueryPairsData");
			Util.postMessage("Getting Property Value for staticNumericSortedQueryPairsData: "
					+ Util.NUMERIC_SORTED_QUERY_PAIR_DATA, MessageType.YELLOW_TEXT, false);
			Util.USE_COLORED_TEXT_ON_CONSOLE = new Boolean(
					prop.getProperty("SolrNightlyBenchmarks.useColoredTextOnConsole"));
			Util.postMessage("Getting Property Value for useColoredTextOnConsole: " + Util.USE_COLORED_TEXT_ON_CONSOLE,
					MessageType.YELLOW_TEXT, false);
			Util.NUMERIC_QUERY_AND_OR_DATA = prop.getProperty("SolrNightlyBenchmarks.staticNumericQueryAndOrTermsData");
			Util.postMessage(
					"Getting Property Value for staticNumericQueryAndOrTermsData: " + Util.NUMERIC_QUERY_AND_OR_DATA,
					MessageType.YELLOW_TEXT, false);
			Util.TEXT_TERM_DATA = prop.getProperty("SolrNightlyBenchmarks.staticTextTermQueryData");
			Util.postMessage("Getting Property Value for staticTextTermQueryData: " + Util.TEXT_TERM_DATA,
					MessageType.YELLOW_TEXT, false);
			Util.TEXT_PHRASE_DATA = prop.getProperty("SolrNightlyBenchmarks.staticTextPhraseQueryData");
			Util.postMessage("Getting Property Value for staticTextPhraseQueryData: " + Util.TEXT_PHRASE_DATA,
					MessageType.YELLOW_TEXT, false);

			if (BenchmarkAppConnector.benchmarkAppDirectory
					.charAt(BenchmarkAppConnector.benchmarkAppDirectory.length() - 1) != File.separator.charAt(0)) {
				Util.postMessage("Corrupt URL for BenchmarkAppConnector.benchmarkAppDirectory Property, correcting ...",
						MessageType.RED_TEXT, false);
				BenchmarkAppConnector.benchmarkAppDirectory += File.separator;
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			// Marking for GC
			input = null;
			prop = null;
		}

	}

	/**
	 * A method used for sending requests to web resources.
	 * 
	 * @param url
	 * @param type
	 * @return
	 */
	public static String getResponse(String url, String type) {

		Client client;
		ClientResponse response;

		try {
			client = Client.create();
			WebResource webResource = client.resource(url);
			response = webResource.accept(type).get(ClientResponse.class);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}

			return response.getEntity(String.class);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Marking for GC
			client = null;
			response = null;
		}

		return "";
	}

	/**
	 * A method used for getting information from the metric API.
	 * 
	 * @param commitID
	 * @param port
	 */
	public static void getEnvironmentInformationFromMetricAPI(String commitID, String port) {

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
	 */
	public static void checkWebAppFiles() {

		Util.postMessage("** Verifying that the Webapp files are present ... ", MessageType.BLUE_TEXT, false);

		File webAppSourceDir = new File("WebAppSource");
		File webAppTargetDir = new File(BenchmarkAppConnector.benchmarkAppDirectory);

		if (!webAppTargetDir.exists()) {
			webAppTargetDir.mkdir();
		}

		try {

			if (!webAppTargetDir.exists()) {
				Util.postMessage("** Webapp target directory not present creating now! ... ", MessageType.RED_TEXT,
						false);
				webAppTargetDir.mkdir();

				if (!new File(
						BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST")
								.exists()) {
					Util.postMessage("** Copying updated/new webapp files ...", MessageType.BLUE_TEXT, false);
					Util.copyFolder(webAppSourceDir, webAppTargetDir);
				}

				File flagFile = new File(
						BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST");
				flagFile.createNewFile();

			} else if (!new File(
					BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST")
							.exists()) {
				Util.postMessage("** Copying updated/new webapp files ...", MessageType.BLUE_TEXT, false);
				Util.copyFolder(webAppSourceDir, webAppTargetDir);

				File flagFile = new File(
						BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST");
				flagFile.createNewFile();

			} else {
				Util.postMessage("** Webapp files seems present, skipping copying webapp files ...",
						MessageType.GREEN_TEXT, false);
			}

		} catch (IOException e) {
			e.printStackTrace();
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
	 */
	public static void copyFolder(File source, File destination) {
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
				try {
					in.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				try {
					out.close();
				} catch (IOException e1) {
					e1.printStackTrace();
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
	 */
	public static void init(String[] args) {

		Util.postMessage("", MessageType.WHITE_TEXT, false);
		Util.postMessage("--------------------------------------------------------------------", MessageType.WHITE_TEXT,
				false);
		Util.postMessage("          	*** Solr Nightly Benchmarks ***  HOLA !!!             ", MessageType.RED_TEXT,
				false);
		Util.postMessage("--------------------------------------------------------------------", MessageType.WHITE_TEXT,
				false);
		Util.postMessage("", MessageType.WHITE_TEXT, false);

		Util.getPropertyValues();

		try {
			argsList = Util.getArgs(args);

			if (argsList.size() == 0) {
				Util.postMessage("** No Parameters defined! [EXITING] ...", MessageType.RED_TEXT, false);
				Util.postMessage(
						"** Please access: https://github.com/viveknarang/lucene-solr/tree/SolrNightlyBenchmarks/dev-tools/SolrNightBenchmarks#possible-parameters ...\n\n",
						MessageType.CYAN_TEXT, false);
				System.exit(0);
			} else {

				int atleastOne = 0;

				if (argsList.contains("--generate-data-file")) {
					atleastOne++;
				}
				if (argsList.contains("--register-commit")) {
					atleastOne++;
				}
				if (argsList.contains("--test-with-number-of-documents")) {

					try {
						Long.parseLong(argsList.get(argsList.indexOf("--test-with-number-of-documents") + 1));
						atleastOne++;
					} catch (Exception e) {
						Util.postMessage(
								"** Parameter value for --test-with-number-of-documents should be a number! [EXITING] ...\n\n",
								MessageType.RED_TEXT, false);
						System.exit(0);
					}

				}
				if (argsList.contains("--silent")) {
					atleastOne++;
				}
				if (argsList.contains("--from-queue")) {
					atleastOne++;
				}
				if (argsList.contains("--latest-commit")) {
					atleastOne++;
				}
				if (argsList.contains("--commit-id")) {

					try {
						argsList.get(argsList.indexOf("--commit-id") + 1);
						atleastOne++;
					} catch (Exception e) {
						Util.postMessage("** Parameter value for --commit-id not defined! [EXITING] ...",
								MessageType.RED_TEXT, false);
						Util.postMessage(
								"** Please access: https://github.com/viveknarang/lucene-solr/tree/SolrNightlyBenchmarks/dev-tools/SolrNightBenchmarks#possible-parameters ...\n\n",
								MessageType.CYAN_TEXT, false);
						System.exit(0);
					}

				}

				if (atleastOne == 0) {
					Util.postMessage("** No Valid Parameters defined! [EXITING] ...", MessageType.RED_TEXT, false);
					Util.postMessage(
							"** Please access: https://github.com/viveknarang/lucene-solr/tree/SolrNightlyBenchmarks/dev-tools/SolrNightBenchmarks#possible-parameters ...\n\n",
							MessageType.CYAN_TEXT, false);
					System.exit(0);
				}

			}

			File datafile = new File(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA);
			if (!datafile.exists()) {
				Util.postMessage("** Data File " + Util.ONEM_TEST_DATA + " Missing! [EXITING] ...\n\n",
						MessageType.RED_TEXT, false);
				System.exit(0);
			}
			datafile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_TERM_DATA);
			if (!datafile.exists()) {
				Util.postMessage("** Data File " + Util.NUMERIC_QUERY_TERM_DATA + " Missing! [EXITING] ...\n\n",
						MessageType.RED_TEXT, false);
				System.exit(0);
			}
			datafile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_PAIR_DATA);
			if (!datafile.exists()) {
				Util.postMessage("** Data File " + Util.NUMERIC_QUERY_PAIR_DATA + " Missing! [EXITING] ...\n\n",
						MessageType.RED_TEXT, false);
				System.exit(0);
			}
			datafile = new File(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_SORTED_QUERY_PAIR_DATA);
			if (!datafile.exists()) {
				Util.postMessage("** Data File " + Util.NUMERIC_SORTED_QUERY_PAIR_DATA + " Missing! [EXITING] ...\n\n",
						MessageType.RED_TEXT, false);
				System.exit(0);
			}

			if (argsList.contains("--generate-data-file")) {
				createTestDataFile("test-data-file-1M.csv", 1000000);
				System.exit(0);
			}

			if (argsList.contains("--register-commit")) {
				Util.postMessage("** SolrNightlyBenchmarks Commit Registry Updater ...", MessageType.WHITE_TEXT, false);
				String commit = Util.getLatestCommitID(Util.LUCENE_SOLR_REPOSITORY_URL);

				if (!BenchmarkAppConnector.isCommitInQueue(commit)) {
					Util.postMessage("** Registering the latest commit in the queue ...", MessageType.RED_TEXT, false);
					BenchmarkAppConnector.writeToWebAppDataFile(commit, "", true, FileType.COMMIT_QUEUE);
				} else {
					Util.postMessage(
							"** Skipping Registering the latest commit in the queue since it already exists ...",
							MessageType.GREEN_TEXT, false);
				}

				Util.postMessage("** SolrNightlyBenchmarks Commit Registry Updater [COMPLETE] now EXIT...",
						MessageType.WHITE_TEXT, false);
				System.exit(0);

			}

			if (argsList.contains("--test-with-number-of-documents")) {
				long numDocuments = Long
						.parseLong(argsList.get(argsList.indexOf("--test-with-number-of-documents") + 1));

				if (numDocuments > 0 && numDocuments <= 1000000) {
					Util.TEST_WITH_NUMBER_OF_DOCUMENTS = numDocuments;
					Util.postMessage("** Number of Documents to test with: " + Util.TEST_WITH_NUMBER_OF_DOCUMENTS,
							MessageType.CYAN_TEXT, false);
				}
			}

			if (argsList.contains("--silent")) {
				Util.postMessage("** Running silently since --silent parameter is set ...", MessageType.BLUE_TEXT,
						false);
				Util.SILENT = true;
			}

			Util.checkWebAppFiles();

			Util.checkBaseAndTempDir();

			Util.setAliveFlag();

			Util.getSystemEnvironmentInformation();

			if (!BenchmarkAppConnector.isRunningFolderEmpty()) {
				Util.postMessage("** It looks like the last test session failed or was aborted ...",
						MessageType.RED_TEXT, false);

				Util.killProcesses("zookeeper");
				Util.killProcesses("Dsolr.jetty.https.port");

				if (!BenchmarkAppConnector.isCloningFolderEmpty()) {
					Util.postMessage("** Looks like a broken clone exists removing it ...", MessageType.RED_TEXT,
							false);
					Util.execute("rm -r -f " + Util.getLocalRepoPath(), Util.getLocalRepoPath());
				}

				Thread.sleep(5000);

				Util.cleanRunDirectory();
				Util.deleteRunningFile();
			}

			if (argsList.contains("--from-queue")) {
				Util.postMessage("** Initiating processing from commit queue ...", MessageType.BLUE_TEXT, false);

				File[] currentCommits = BenchmarkAppConnector.getRegisteredCommitsFromQueue();
				int length = currentCommits.length;

				if (length == 0) {
					Util.postMessage("** Commit queue empty! [EXIT] ...", MessageType.BLUE_TEXT, false);
				} else {
					for (int i = 0; i < length; i++) {

						String commitIDFromQueue = currentCommits[i].getName();
						Util.COMMIT_ID = commitIDFromQueue;
						String lastRun = BenchmarkAppConnector.getLastRunCommitID();

						if (commitIDFromQueue.equals(lastRun)) {
							Util.postMessage(
									"** The commit: " + commitIDFromQueue + " has already been processed skipping ...",
									MessageType.RED_TEXT, false);
							BenchmarkAppConnector.deleteCommitFromQueue(commitIDFromQueue);
						} else {
							Util.createIsRunningFile();
							Util.postMessage("** Processing benchmarks for commit: " + commitIDFromQueue,
									MessageType.GREEN_TEXT, false);
							TestPlans.execute();
							BenchmarkAppConnector.publishDataForWebApp();
							BenchmarkReportData.reset();
							BenchmarkAppConnector.deleteCommitFromQueue(commitIDFromQueue);
							System.gc();
						}

					}
					Util.postMessage("** Processing from commit queue [COMPLETE] ...", MessageType.BLUE_TEXT, false);
				}
			} else if (argsList.contains("--latest-commit")) {

				Util.COMMIT_ID = Util.getLatestCommitID(Util.LUCENE_SOLR_REPOSITORY_URL);
				Util.postMessage("The latest commit ID is: " + Util.COMMIT_ID, MessageType.YELLOW_TEXT, false);

				TestPlans.execute();
				BenchmarkAppConnector.publishDataForWebApp();
				BenchmarkReportData.reset();
			} else if (argsList.contains("--commit-id")) {

				Util.COMMIT_ID = argsList.get(argsList.indexOf("--commit-id") + 1);
				Util.postMessage("** Executing benchmarks with commit: " + Util.COMMIT_ID, MessageType.BLUE_TEXT,
						false);
				TestPlans.execute();
				BenchmarkAppConnector.publishDataForWebApp();
				BenchmarkReportData.reset();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * A method used for doing the cleanup after the benchmarks cycle end.
	 */
	public static void destroy() {

		try {

			if (argsList.contains("--clean-up")) {
				Util.postMessage("** Initiating Housekeeping activities! ... ", MessageType.RED_TEXT, false);
				Util.execute("rm -r -f " + Util.DOWNLOAD_DIR, Util.DOWNLOAD_DIR);
			}

			Util.setDeadFlag();
			Util.createLastRunFile();
			Util.deleteRunningFile();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * A method used for creating the last run file.
	 */
	public static void createLastRunFile() {

		BenchmarkAppConnector.deleteFolder(FileType.LAST_RUN_COMMIT);
		BenchmarkAppConnector.writeToWebAppDataFile(Util.COMMIT_ID, "", true, FileType.LAST_RUN_COMMIT);

	}

	/**
	 * A method used for creating the running flag file.
	 */
	public static void createIsRunningFile() {

		BenchmarkAppConnector.deleteFolder(FileType.IS_RUNNING_FILE);
		BenchmarkAppConnector.writeToWebAppDataFile(Util.COMMIT_ID, "", true, FileType.IS_RUNNING_FILE);

	}

	/**
	 * A method used for deleting the running file.
	 */
	public static void deleteRunningFile() {
		BenchmarkAppConnector.deleteFolder(FileType.IS_RUNNING_FILE);
	}

	/**
	 * A method used for cleaning the run directory.
	 */
	public static void cleanRunDirectory() {
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
	 */
	public static void createTestDataFile(String fileName, int numberOfDocuments) {
		Util.postMessage("** Preparing 4k text documents", MessageType.WHITE_TEXT, false);
		for (int i = 0; i < numberOfDocuments; i++) {
			if (i % 100 == 0) {
				Util.postMessageOnLine("|");
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
	 */
	public static void createNumericSortedQueryDataFile(String fileName, int numberOfDocuments) {
		Util.postMessage("** Preparing sorted numeric query data ...", MessageType.WHITE_TEXT, false);
		for (int i = 0; i < numberOfDocuments; i++) {
			if (i % 100 == 0) {
				Util.postMessageOnLine("|");
			}

			Random r = new Random();
			int number = r.nextInt((1000000 - 100));

			String line = number + "," + (number + 100);

			BenchmarkAppConnector.writeToWebAppDataFile(fileName, line, false, FileType.TEST_ENV_FILE);
		}
		Util.postMessage("** Preparation [COMPLETE] ...", MessageType.WHITE_TEXT, false);
	}

	/**
	 * A method used for locating and killing unused processes.
	 * 
	 * @param lookFor
	 */
	public static void killProcesses(String lookFor) {

		Util.postMessage("** Searching and killing " + lookFor + " process(es) ...", MessageType.RED_TEXT, false);

		BufferedReader reader;
		String line = "";

		try {
			String[] cmd = { "/bin/sh", "-c", "ps -ef | grep " + lookFor + " | awk '{print $2}'" };
			reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(cmd).getInputStream()));

			while ((line = reader.readLine()) != null) {

				line = line.trim();
				Util.postMessage("** Found " + lookFor + " Running with PID " + line + " Killing now ..",
						MessageType.RED_TEXT, false);
				Runtime.getRuntime().exec("kill -9 " + line);
			}

			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// Marking for GC
			reader = null;
			line = null;
		}

	}

	/**
	 * A method used for creating test data file using Wikipedia data.
	 */
	public static void CreateWikiDataFile() {

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

						Util.postMessageOnLine("\r" + id);
					}
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}