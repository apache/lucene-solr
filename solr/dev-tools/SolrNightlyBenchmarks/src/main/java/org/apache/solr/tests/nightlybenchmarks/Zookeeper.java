package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class Zookeeper {

	public static String zooCommand;

	static {

		zooCommand = System.getProperty("os.name") != null && System.getProperty("os.name").startsWith("Windows")
				? "bin" + File.separator + "zkServer.cmd " : "bin" + File.separator + "zkServer.sh ";

	}

	Zookeeper() throws IOException {
		super();
		this.install();
	}

	private void install() throws IOException {

		Util.postMessage("** Installing Zookeeper Node ...", MessageType.ACTION, false);

		File base = new File(Util.ZOOKEEPER_DIR);
		if (!base.exists()) {
			base.mkdir();
			base.setExecutable(true);
		}

		File release = new File(Util.TEMP_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz");
		if (!release.exists()) {

			String fileName = null;
			URL link = null;
			InputStream in = null;
			FileOutputStream fos = null;

			try {

				Util.postMessage("** Attempting to download zookeeper release ..." + " : " + Util.ZOOKEEPER_RELEASE,
						MessageType.ACTION, true);
				fileName = "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz";
				link = new URL(Util.ZOOKEEPER_DOWNLOAD_URL + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + fileName);
				Util.postMessage(Util.ZOOKEEPER_DOWNLOAD_URL + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + fileName,
						MessageType.ACTION, true);
				in = new BufferedInputStream(link.openStream());
				fos = new FileOutputStream(Util.TEMP_DIR + fileName);
				byte[] buf = new byte[1024 * 1024]; // 1mb blocks
				int n = 0;
				long size = 0;
				while (-1 != (n = in.read(buf))) {
					size += n;
					Util.postMessageOnLine(size + " ");
					fos.write(buf, 0, n);
				}
				fos.close();
				in.close();

			} catch (Exception e) {

				Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, false);

			}
		} else {
			Util.postMessage("** Release present nothing to download ...",
					MessageType.RESULT_SUCCESS, false);		
		}

		File urelease = new File(Util.TEMP_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE);
		if (!urelease.exists()) {

			{
				Runtime rt = Runtime.getRuntime();
				Process proc = null;
				ProcessStreamReader errorGobbler = null;
				ProcessStreamReader outputGobbler = null;

				try {

					proc = rt.exec("tar -xf " + Util.TEMP_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz" + " -C "
							+ Util.ZOOKEEPER_DIR);

					errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
					outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

					errorGobbler.start();
					outputGobbler.start();
					proc.waitFor();

				} catch (Exception e) {

					Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);

				}
			}

			Runtime rt = Runtime.getRuntime();
			Process proc = null;
			ProcessStreamReader errorGobbler = null;
			ProcessStreamReader outputGobbler = null;

			try {

				proc = rt.exec("mv " + Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + "conf"
						+ File.separator + "zoo_sample.cfg " + Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE
						+ File.separator + "conf" + File.separator + "zoo.cfg");

				errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
				outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

				errorGobbler.start();
				outputGobbler.start();
				proc.waitFor();

			} catch (Exception e) {

				Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);

			}

		} else {
			Util.postMessage("** Release extracted already nothing to do ..." + " : " + Util.ZOOKEEPER_RELEASE,
					MessageType.RESULT_SUCCESS, false);		
		}
	}

	@SuppressWarnings("finally")
	public int start() {

		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;

		try {

			Util.postMessage("** Attempting to start zookeeper ...", MessageType.ACTION, false);

			new File(Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand)
					.setExecutable(true);
			proc = rt.exec(Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand + " start");

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();

			return proc.exitValue();

		} catch (Exception e) {

			Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);

		} finally {

			return proc.exitValue();

		}

	}

	@SuppressWarnings("finally")
	public int stop() {

		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;

		try {

			Util.postMessage("** Attempting to stop zookeeper ...", MessageType.ACTION, false);

			new File(Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand)
					.setExecutable(true);
			proc = rt.exec(Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand + " stop");

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();

			return proc.exitValue();

		} catch (Exception e) {

			Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, false);

		} finally {

			return proc.exitValue();

		}

	}

	@SuppressWarnings("finally")
	public int clean() throws IOException, InterruptedException {

		Util.postMessage("** Deleting directory for zookeeper data ...", MessageType.ACTION, false);
		Runtime rt = Runtime.getRuntime();
		Process proc = null;
		ProcessStreamReader errorGobbler = null;
		ProcessStreamReader outputGobbler = null;

		try {

			proc = rt.exec("rm -r -f " + Util.ZOOKEEPER_DIR);

			errorGobbler = new ProcessStreamReader(proc.getErrorStream(), "ERROR");
			outputGobbler = new ProcessStreamReader(proc.getInputStream(), "OUTPUT");

			errorGobbler.start();
			outputGobbler.start();
			proc.waitFor();
			return proc.exitValue();

		} catch (Exception e) {

			Util.postMessage(e.getMessage(), MessageType.RESULT_ERRROR, true);

		} finally {

			return proc.exitValue();

		}

	}

	public String getZookeeperIp() {
		return Util.ZOOKEEPER_IP;
	}

	public String getZookeeperPort() {
		return Util.ZOOKEEPER_PORT;
	}

}