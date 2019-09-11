package org.apache.solr.packagemanager;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
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

	Map<String, PluginWrapper> plugins = null;

	@Override
	public List<PluginWrapper> getPlugins() {
		List<PluginWrapper> ret = new ArrayList<PluginWrapper>();
		plugins = new HashMap<String, PluginWrapper>();
		try {
			String clusterPropsZnode = IOUtils.toString(new URL("http://localhost:8983/solr/admin/zookeeper?detail=true&path=/clusterprops.json&wt=json").openStream(), "UTF-8");
			String clusterPropsJson = ((Map)new Gson().fromJson(clusterPropsZnode, Map.class).get("znode")).get("data").toString();
			Map packages = (Map)new Gson().fromJson(clusterPropsJson, Map.class).get("package");

			for (Object packageName: packages.keySet()) {
				Map pkg = (Map)packages.get(packageName);
				PluginDescriptor descriptor = new SolrPluginDescriptor(pkg.get("name").toString(), null, pkg.get("version").toString(), (List)pkg.get("setup-commands"));
				PluginWrapper wrapper = new PluginWrapper(this, descriptor, null, null);
				plugins.put(packageName.toString(), wrapper);
				ret.add(wrapper);
			}
		} catch (IOException e) {
			if (plugins == null) plugins = Collections.emptyMap();
		}
		return ret;
	}

	public boolean deployPlugin(String pluginId, List<String> collections) {
		PluginWrapper plugin = getPlugin(pluginId);
		System.out.println(((SolrPluginDescriptor)plugin.getDescriptor()).getSetupCommands());
		for (String collection: collections) {
			for (String cmd: ((SolrPluginDescriptor)plugin.getDescriptor()).getSetupCommands()) {
				System.out.println("Executing " + cmd + " for collection:" + collection);
			}
		}
		return true;
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
		if (plugins == null) getPlugins();
		return plugins.get(pluginId);
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
