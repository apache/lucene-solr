package org.apache.solr.tests.nightlybenchmarks;

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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.ZipInputStream;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

enum MessageType {
	PROCESS, ACTION, RESULT_SUCCESS, RESULT_ERRROR, GENERAL
};

public class Util {
	
	public static String WORK_DIRECTORY = System.getProperty("user.dir");
	public static String DNAME = "SolrNightlyBenchmarks";
	public static String BASE_DIR = WORK_DIRECTORY + File.separator + DNAME + File.separator;
	public static String TEMP_DIR = BASE_DIR + "temp" + File.separator;
	public static String ZOOKEEPER_DOWNLOAD_URL = "http://www.us.apache.org/dist/zookeeper/";
	public static String ZOOKEEPER_RELEASE = "3.4.6";
	public static String ZOOKEEPER_DIR = BASE_DIR + "ZOOKEEPER" + File.separator;
	public static String ZOOKEEPER_IP = "127.0.0.1";
	public static String ZOOKEEPER_PORT = "2181";

	final static Logger logger = Logger.getLogger(Util.class);
	public static String gitRepositoryPath;
	public static String commitId;

	public static void postMessage(String message, MessageType type, boolean printInLog) {

		String ANSI_RESET = "\u001B[0m";
		String ANSI_RED = "\u001B[31m";
		String ANSI_GREEN = "\u001B[32m";
		String ANSI_YELLOW = "\u001B[33m";
		String ANSI_BLUE = "\u001B[34m";
		String ANSI_WHITE = "\u001B[37m";

		if (type.equals(MessageType.ACTION)) {
			System.out.println(ANSI_WHITE + message + ANSI_RESET);
		} else if (type.equals(MessageType.GENERAL)) {
			System.out.println(ANSI_BLUE + message + ANSI_RESET);
		} else if (type.equals(MessageType.PROCESS)) {
			System.out.println(ANSI_YELLOW + message + ANSI_RESET);
		} else if (type.equals(MessageType.RESULT_ERRROR)) {
			System.out.println(ANSI_RED + message + ANSI_RESET);
		} else if (type.equals(MessageType.RESULT_SUCCESS)) {
			System.out.println(ANSI_GREEN + message + ANSI_RESET);
		}

		if (printInLog) {
			logger.info(message);
		}

	}

	public static int execute(String command, String workingDirectoryPath) {
		Util.postMessage("Executing: " + command, MessageType.ACTION, true);
		Util.postMessage("Working dir: " + workingDirectoryPath, MessageType.ACTION, true);
		File workingDirectory = new File(workingDirectoryPath);

		workingDirectory.setExecutable(true);

		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;

		try {
			proc = rt.exec(command, new String[] {}, workingDirectory);

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();
			return proc.exitValue();
		} catch (Exception e) {
			Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);
			return -1;
		}
	}

	@SuppressWarnings("finally")
	public static int deleteDirectory(String directory) throws IOException, InterruptedException {

		postMessage("Deleting directory: " + directory, MessageType.ACTION, true);
		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;

		try {

			proc = rt.exec("rm -r -f " + directory);

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();
			return proc.exitValue();

		} catch (Exception e) {

			postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);
			return -1;

		} finally {

			proc.destroy();
			return proc.exitValue();

		}

	}

	@SuppressWarnings("finally")
	public static int createDirectory(String directory) throws IOException, InterruptedException {

		postMessage("Creating directory: " + directory, MessageType.ACTION, true);
		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;

		try {

			proc = rt.exec("mkdir " + directory);

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();
			return proc.exitValue();

		} catch (Exception e) {

			postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);
			return -1;

		} finally {

			proc.destroy();
			return proc.exitValue();

		}
	}

	public static void postMessageOnLine(String message) {

		System.out.print(message);

	}

	public static void checkBaseAndTempDir() {
		BasicConfigurator.configure();
		File baseDirectory = new File(BASE_DIR);
		baseDirectory.mkdir();
		File tempDirectory = new File(TEMP_DIR);
		tempDirectory.mkdir();
	}

	public static int getFreePort() {

		int port = ThreadLocalRandom.current().nextInt(10000, 60000);
		Util.postMessage("Looking for a free port ... Checking availability of port number: " + port,
				MessageType.ACTION, true);
		ServerSocket serverSocket = null;
		DatagramSocket datagramSocket = null;
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);
			datagramSocket = new DatagramSocket(port);
			datagramSocket.setReuseAddress(true);
			Util.postMessage("Port " + port + " is free to use. Using this port !!", MessageType.RESULT_SUCCESS, true);
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
		}

		Util.postMessage("Port " + port + " looks occupied trying another port number ... ", MessageType.RESULT_ERRROR,
				true);
		return getFreePort();
	}

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

			Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);

		} finally {

			bos.close();

		}
	}

	public static void extract(String filename, String filePath) throws IOException {
		Util.postMessage("** Attempting to unzip the downloaded release ...", MessageType.ACTION, true);
		try {
			ZipFile zip = new ZipFile(filename);
			zip.extractAll(filePath);
		} catch (ZipException ex) {
			throw new IOException(ex);
		}
	}

	public static String getLatestCommitID() throws IOException {
		Util.postMessage("** Getting the latest commit ID from remote repository", MessageType.GENERAL, false);
		return new BufferedReader(new InputStreamReader(
				Runtime.getRuntime().exec("git ls-remote https://github.com/apache/lucene-solr HEAD").getInputStream()))
						.readLine().split("HEAD")[0].trim();
	}

	public static String getCommitInformation() {
		Util.postMessage("** Getting the latest commit Information from local repository", MessageType.GENERAL, false);
		File directory = new File(Util.gitRepositoryPath);
		directory.setExecutable(true);
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(Runtime.getRuntime()
					.exec("git show --no-patch " + Util.commitId, new String[] {}, directory).getInputStream()));

			reader.readLine();
			String returnString;
			returnString = reader.readLine().replaceAll("<", " ").replaceAll(">", " ").trim() + ", "
					+ reader.readLine().trim();
			reader.readLine();
			returnString += ", " + reader.readLine().trim();
			return returnString;

		} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
	}

	public static void getSystemEnvironmentInformation() {

		Util.postMessage("** Getting the test environment information", MessageType.GENERAL, false);

		BufferedReader reader;
		try {
			String returnString = "";
			String line = "";

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
		}
	}

	public static Map<String, Long> getMemoryState() {

		Map<String, Long> aMap = new HashMap<String, Long>();

		try {

			String line = "";
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(Runtime.getRuntime().exec("cat /proc/meminfo").getInputStream()));

			while ((line = reader.readLine()) != null) {
				aMap.put(line.split(":")[0].trim(), Long.parseLong(line.split(":")[1].trim().replaceAll("[^0-9]", "")));
			}

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return aMap;

	}

	public static void getProperties() {

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
					MessageType.PROCESS, false);
			SolrClient.solrCommitHistoryData = prop.getProperty("SolrNightlyBenchmarks.solrCommitHistoryData");
			Util.postMessage("Getting Property Value for solrCommitHistoryData: " + SolrClient.solrCommitHistoryData,
					MessageType.PROCESS, false);
			SolrClient.amazonFoodDataLocation = prop.getProperty("SolrNightlyBenchmarks.amazonFoodDataLocation");
			Util.postMessage("Getting Property Value for amazonFoodDataLocation: " + SolrClient.amazonFoodDataLocation,
					MessageType.PROCESS, false);		
			MetricEstimation.metricsURL = prop.getProperty("SolrNightlyBenchmarks.metricsURL");
			Util.postMessage("Getting Property Value for metricsURL: " + MetricEstimation.metricsURL,
					MessageType.PROCESS, false);
			Util.ZOOKEEPER_DOWNLOAD_URL = prop.getProperty("SolrNightlyBenchmarks.zookeeperDownloadURL");
			Util.postMessage("Getting Property Value for zookeeperDownloadURL: " + Util.ZOOKEEPER_DOWNLOAD_URL,
					MessageType.PROCESS, false);
			Util.ZOOKEEPER_RELEASE = prop.getProperty("SolrNightlyBenchmarks.zookeeperDownloadVersion");
			Util.postMessage("Getting Property Value for zookeeperDownloadVersion: " + Util.ZOOKEEPER_RELEASE,
					MessageType.PROCESS, false);
			Util.ZOOKEEPER_IP = prop.getProperty("SolrNightlyBenchmarks.zookeeperHostIp");
			Util.postMessage("Getting Property Value for zookeeperHostIp: " + Util.ZOOKEEPER_IP,
					MessageType.PROCESS, false);
			Util.ZOOKEEPER_PORT = prop.getProperty("SolrNightlyBenchmarks.zookeeperHostPort");
			Util.postMessage("Getting Property Value for zookeeperHostPort: " + Util.ZOOKEEPER_PORT,
					MessageType.PROCESS, false);
			

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
		}

	}

	public static String getResponse(String url, String type) {

		try {
			Client client = Client.create();
			WebResource webResource = client.resource(url);

			ClientResponse response = webResource.accept(type).get(ClientResponse.class);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}

			return response.getEntity(String.class);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	public static void getEnvironmentInformationFromMetricAPI(String commitID, String port) {
		
		String response = Util.getResponse("http://localhost:" + port + "/solr/admin/metrics?wt=json&group=jvm", MediaType.APPLICATION_JSON);
		JSONObject jsonObject = (JSONObject) JSONValue.parse(response);
		
		String printString = "";
		printString += "Memory Heap Committed:    <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.committed") + " Bytes</b><br/>\n";
		printString += "Memory Heap Init:         <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.init") + " Bytes</b><br/>\n";
		printString += "Memory Heap Max:          <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.max") + " Bytes</b><br/>\n";

		printString += "Memory Non-Heap Committed:<b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.non-heap.committed") + " Bytes</b><br/>\n";
		printString += "Memory Non-Heap Init:     <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.non-heap.init") + " Bytes</b><br/>\n";
		printString += "Memory Non-Heap Max:      <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.non-heap.max") + " Bytes</b><br/>\n";

		printString += "Memory Total Committed:   <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.total.committed") + " Bytes</b><br/>\n";
		printString += "Memory Total Init:        <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.total.init") + " Bytes</b><br/>\n";
		printString += "Memory Total Max:         <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.total.max") + " Bytes</b><br/>\n";
		
		printString += "Total Physical Memory:    <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.totalPhysicalMemorySize") + " Bytes</b><br/>\n";
		printString += "Total Swap Space:         <b>" + ((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.totalSwapSpaceSize") + " Bytes</b><br/>\n";
		
		
		BenchmarkAppConnector.writeToWebAppDataFile(commitID + "_" + FileType.TEST_ENV_FILE  +  "_dump.csv", printString, true, FileType.TEST_ENV_FILE);
		
	}	
	
	public static void cleanUpSrcDirs() {
		
		Util.postMessage("** Initiating Housekeeping activities! ... ", MessageType.RESULT_ERRROR, false);
		
		try {
			Util.deleteDirectory(TEMP_DIR);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void checkWebAppFiles() {
		
		Util.postMessage("** Verifying that the Webapp files are present ... ", MessageType.GENERAL, false);
		
		File webAppSourceDir = new File("WebAppSource");
		File webAppTargetDir = new File(BenchmarkAppConnector.benchmarkAppDirectory);

		try {
		
			if(!webAppTargetDir.exists()) {
				Util.postMessage("** Webapp target directory not present creating now! ... ", MessageType.RESULT_ERRROR, false);
				webAppTargetDir.mkdir();
	
				if (!new File(BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST").exists()) {
					Util.postMessage("** Copying updated/new webapp files ...", MessageType.GENERAL, false);
					Util.copyFolder(webAppSourceDir, webAppTargetDir);
				}
				
					File flagFile = new File(BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST");
					flagFile.createNewFile();
			
			} else if (!new File(BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST").exists()) {
				Util.postMessage("** Copying updated/new webapp files ...", MessageType.GENERAL, false);
				Util.copyFolder(webAppSourceDir, webAppTargetDir);
				
					File flagFile = new File(BenchmarkAppConnector.benchmarkAppDirectory + File.separator + "UPDATED_WEB_APP_FILES_EXIST");
					flagFile.createNewFile();
					
			} else {
				Util.postMessage("** Webapp files seems present, skipping copying webapp files ...", MessageType.RESULT_SUCCESS, false);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}


	}
	
	public static void copyFolder(File source, File destination)
	{
	    if (source.isDirectory())
	    {
	        if (!destination.exists())
	        {
	            destination.mkdirs();
	        }

	        String files[] = source.list();

	        for (String file : files)
	        {
	            File srcFile = new File(source, file);
	            File destFile = new File(destination, file);

	            copyFolder(srcFile, destFile);
	        }
	    }
	    else
	    {
	        InputStream in = null;
	        OutputStream out = null;

	        try
	        {
	            in = new FileInputStream(source);
	            out = new FileOutputStream(destination);

	            byte[] buffer = new byte[1024];

	            int length;
	            while ((length = in.read(buffer)) > 0)
	            {
	                out.write(buffer, 0, length);
	            }
	        }
	        catch (Exception e)
	        {
	            try
	            {
	                in.close();
	            }
	            catch (IOException e1)
	            {
	                e1.printStackTrace();
	            }

	            try
	            {
	                out.close();
	            }
	            catch (IOException e1)
	            {
	                e1.printStackTrace();
	            }
	        }
	    }
	}	
}