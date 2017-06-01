package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ProcessStreamReader extends Thread {

	InputStream is;
	String type;

	ProcessStreamReader(InputStream is, String type) {
		this.is = is;
		this.type = type;
	}

	public void run() {
		try {
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null) {
				Util.postMessage(" >> " + line, MessageType.GREEN_TEXT, false);
			}

		} catch (IOException ioe) {
			Util.postMessage(ioe.getMessage(), MessageType.RED_TEXT, false);
		}
	}

}
