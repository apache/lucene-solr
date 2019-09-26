package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.packagemanager.SolrPluginInfo.Metadata;
import org.apache.solr.packagemanager.SolrPluginInfo.Plugin;
import org.pf4j.AbstractPluginManager;
import org.pf4j.DefaultVersionManager;
import org.pf4j.ExtensionFactory;
import org.pf4j.ExtensionFinder;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginDescriptorFinder;
import org.pf4j.PluginFactory;
import org.pf4j.PluginLoader;
import org.pf4j.PluginRepository;
import org.pf4j.PluginState;
import org.pf4j.PluginStateListener;
import org.pf4j.PluginStatusProvider;
import org.pf4j.PluginWrapper;
import org.pf4j.RuntimeMode;
import org.pf4j.VersionManager;

import com.google.gson.Gson;

public 	class SolrPluginManager extends AbstractPluginManager {

	final VersionManager versionManager;
	
	public SolrPluginManager(File repo) {
		versionManager = new DefaultVersionManager();
	}

	Map<String, PluginWrapper> packages = null;

	Metadata fetchMetadata(String blobSha256) throws MalformedURLException, IOException {
    String metadataJson = 
        IOUtils.toString(new URL("http://localhost:8983/api/node/blob"+"/"+blobSha256).openStream(), "UTF-8");
    System.out.println("Fetched metadata blob: "+metadataJson);
    Metadata metadata = new Gson().fromJson(metadataJson, Metadata.class);
    System.out.println("Now metadata: "+metadata);
    return metadata;
	}
	
	@Override
	public List<PluginWrapper> getPlugins() {
	  System.out.println("Getting packages from clusterprops...");
		List<PluginWrapper> ret = new ArrayList<PluginWrapper>();
		packages = new HashMap<String, PluginWrapper>();
		try {
			String clusterPropsZnode = IOUtils.toString(new URL("http://localhost:8983/solr/admin/zookeeper?detail=true&path=/clusterprops.json&wt=json").openStream(), "UTF-8");
			String clusterPropsJson = ((Map)new Gson().fromJson(clusterPropsZnode, Map.class).get("znode")).get("data").toString();
			Map packagesJson = (Map)new Gson().fromJson(clusterPropsJson, Map.class).get("packages");

			System.out.println("clusterprops are: "+clusterPropsJson);
			for (Object packageName: packagesJson.keySet()) {
				Map pkg = (Map)packagesJson.get(packageName);
				List<Plugin> solrplugins = fetchMetadata(pkg.get("metadata").toString()).plugins;
				PluginDescriptor descriptor = new SolrPluginDescriptor(pkg.get("name").toString(), null, 
				    pkg.get("version").toString(), solrplugins);
				PluginWrapper wrapper = new PluginWrapper(this, descriptor, null, null);
				packages.put(packageName.toString(), wrapper);
				ret.add(wrapper);
			}
		} catch (IOException e) {
		  e.printStackTrace();
			if (packages == null) packages = Collections.emptyMap(); // nocommit can't happen
		}
		return ret;
	}

	public boolean deployInstallPlugin(String pluginId, List<String> collections) {
	  PluginWrapper plugin = getPlugin(pluginId);
	  
	  System.out.println("1: "+plugin);
    System.out.println("2: "+plugin.getDescriptor());
    System.out.println("3: "+((SolrPluginDescriptor)plugin.getDescriptor()).getPlugins());
	  for (Plugin p: ((SolrPluginDescriptor)plugin.getDescriptor()).getPlugins()) {
	    System.out.println(p.setupCommand);
	    for (String collection: collections) {
	      System.out.println("Executing " + p.setupCommand + " for collection:" + collection);
	      postJson("http://localhost:8983/solr/"+collection+"/config", p.setupCommand);
	    }
	  }
	  return true;
	}

	 public boolean deployUpdatePlugin(String pluginId, List<String> collections) {
	    PluginWrapper plugin = getPlugin(pluginId);
	    for (Plugin p: ((SolrPluginDescriptor)plugin.getDescriptor()).getPlugins()) {

	    System.out.println(p.updateCommand);
	    for (String collection: collections) {
	        System.out.println("Executing " + p.updateCommand + " for collection:" + collection);
	        postJson("http://localhost:8983/solr/"+collection+"/config", p.updateCommand);
	    }
	    }
	    return true;
	  }


	 private void postJson(String url, String postBody) {
	    try (CloseableHttpClient client = HttpClients.createDefault();) {
	      HttpPost httpPost = new HttpPost(url);
	      StringEntity entity = new StringEntity(postBody);
	      httpPost.setEntity(entity);
	      httpPost.setHeader("Accept", "application/json");
	      httpPost.setHeader("Content-type", "application/json");

	      CloseableHttpResponse response = client.execute(httpPost);

	      try {
	        HttpEntity rspEntity = response.getEntity();
	        if (rspEntity != null) {
	          InputStream is = rspEntity.getContent();
	          StringWriter writer = new StringWriter();
	          IOUtils.copy(is, writer, "UTF-8");
	          String results = writer.toString();
	          System.out.println(results);
	        }
	      } catch (IOException e) {
	        e.printStackTrace();
	      }
	    } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
	 }
	 
	@Override
	public List<PluginWrapper> getPlugins(PluginState pluginState) {
		return null;
	}

	@Override
	public List<PluginWrapper> getResolvedPlugins() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PluginWrapper> getUnresolvedPlugins() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PluginWrapper> getStartedPlugins() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PluginWrapper getPlugin(String pluginId) {
		/*if (packages == null)*/ getPlugins();
		return packages.get(pluginId);
	}

	@Override
	public void loadPlugins() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String loadPlugin(Path pluginPath) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startPlugins() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PluginState startPlugin(String pluginId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stopPlugins() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PluginState stopPlugin(String pluginId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean unloadPlugin(String pluginId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean disablePlugin(String pluginId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean enablePlugin(String pluginId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deletePlugin(String pluginId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ClassLoader getPluginClassLoader(String pluginId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<T> getExtensions(Class<T> type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> List<T> getExtensions(Class<T> type, String pluginId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List getExtensions(String pluginId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getExtensionClassNames(String pluginId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExtensionFactory getExtensionFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RuntimeMode getRuntimeMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PluginWrapper whichPlugin(Class<?> clazz) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addPluginStateListener(PluginStateListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePluginStateListener(PluginStateListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSystemVersion(String version) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSystemVersion() {
		// TODO Auto-generated method stub
		return "0.0.0";
	}

	@Override
	public Path getPluginsRoot() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VersionManager getVersionManager() {
		return versionManager;
	}

	@Override
	protected PluginRepository createPluginRepository() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected PluginFactory createPluginFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ExtensionFactory createExtensionFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected PluginDescriptorFinder createPluginDescriptorFinder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ExtensionFinder createExtensionFinder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected PluginStatusProvider createPluginStatusProvider() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected PluginLoader createPluginLoader() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected VersionManager createVersionManager() {
		// TODO Auto-generated method stub
		return new DefaultVersionManager();
	}
	
}
