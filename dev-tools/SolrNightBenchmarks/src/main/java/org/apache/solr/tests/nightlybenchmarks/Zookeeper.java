package org.apache.solr.tests.nightlybenchmarks;

import java.io.File;
import java.io.IOException;

enum ZookeeperAction {
	ZOOKEEPER_START, ZOOKEEPER_STOP, ZOOKEEPER_CLEAN
}

public class Zookeeper {

	public static String zooCommand;
	public static String zooCleanCommand;

	static {

		zooCommand = System.getProperty("os.name") != null && System.getProperty("os.name").startsWith("Windows")
				? "bin" + File.separator + "zkServer.cmd " : "bin" + File.separator + "zkServer.sh ";

	}

	Zookeeper() throws IOException {
		super();
		this.install();
	}

	private void install() throws IOException {

		Util.postMessage("** Installing Zookeeper Node ...", MessageType.WHITE_TEXT, false);

		File base = new File(Util.ZOOKEEPER_DIR);
		if (!base.exists()) {
			base.mkdir();
			base.setExecutable(true);
		}

		File release = new File(Util.TEMP_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz");
		if (!release.exists()) {

			Util.postMessage("** Attempting to download zookeeper release ..." + " : " + Util.ZOOKEEPER_RELEASE,
					MessageType.WHITE_TEXT, true);
			String fileName = "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz";

			Util.download(
					Util.ZOOKEEPER_DOWNLOAD_URL + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + fileName,
					Util.TEMP_DIR + fileName);

		} else {
			Util.postMessage("** Release present nothing to download ...", MessageType.GREEN_TEXT, false);
		}

		File urelease = new File(Util.TEMP_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE);
		if (!urelease.exists()) {

			Util.execute("tar -xf " + Util.TEMP_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + ".tar.gz" + " -C "
					+ Util.ZOOKEEPER_DIR, Util.ZOOKEEPER_DIR);

			Util.execute(
					"mv " + Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + "conf"
							+ File.separator + "zoo_sample.cfg " + Util.ZOOKEEPER_DIR + "zookeeper-"
							+ Util.ZOOKEEPER_RELEASE + File.separator + "conf" + File.separator + "zoo.cfg",
					Util.ZOOKEEPER_DIR);

		} else {
			Util.postMessage("** Release extracted already nothing to do ..." + " : " + Util.ZOOKEEPER_RELEASE,
					MessageType.GREEN_TEXT, false);
		}
	}

	public int doAction(ZookeeperAction action) {

		new File(Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand)
				.setExecutable(true);

		if (action == ZookeeperAction.ZOOKEEPER_START) {
			return Util.execute(
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand + " start",
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator);
		} else if (action == ZookeeperAction.ZOOKEEPER_STOP) {
			return Util.execute(
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator + zooCommand + " stop",
					Util.ZOOKEEPER_DIR + "zookeeper-" + Util.ZOOKEEPER_RELEASE + File.separator);
		} else if (action == ZookeeperAction.ZOOKEEPER_CLEAN) {
			Util.execute("rm -r -f " + Util.ZOOKEEPER_DIR, Util.ZOOKEEPER_DIR);
			return Util.execute("rm -r -f /tmp/zookeeper/", "/tmp/zookeeper/");
		}

		return -1;
	}

	public String getZookeeperIp() {
		return Util.ZOOKEEPER_IP;
	}

	public String getZookeeperPort() {
		return Util.ZOOKEEPER_PORT;
	}

}