# An instance of Plugin is created for each plugin loaded by Rails, and
# stored in the <tt>Rails.plugins</tt> PluginList 
# (see Engines::RailsExtensions::RailsInitializer for more details).
#
# Once the engines plugin is loaded, other plugins can take advantage of
# their own instances by accessing either Engines.current, or the preferred mechanism
#
#   Rails.plugins[:plugin_name]
#
# Useful properties of this object include Plugin#version, which plugin developers
# can set in their <tt>init.rb</tt> scripts:
#
#    Rails.plugins[:my_plugin].version = "1.4.2"
#
# Plugin developers can also access the contents of their <tt>about.yml</tt> files
# via Plugin#about, which returns a Hash if the <tt>about.yml</tt> file exists for
# this plugin. Note that if <tt>about.yml</tt> contains a "version" key, it will 
# automatically be loaded into the <tt>version</tt> attribute described above.
#
# If this plugin contains paths in directories other than <tt>app/controllers</tt>,
# <tt>app/helpers</tt>, <tt>app/models</tt> and <tt>components</tt>, authors can
# declare this by adding extra paths to #code_paths:
#
#    Rails.plugin[:my_plugin].code_paths << "app/sweepers" << "vendor/my_lib"
#
# Other properties of the Plugin instance can also be set.
class Plugin
  
  # The name of this plugin
  attr_accessor :name

  # The directory in which this plugin is located
  attr_accessor :root
  
  # The version of this plugin
  attr_accessor :version
  
  # The about.yml information as a Hash, if it exists
  attr_accessor :about
  
  # Plugins can add code paths to this attribute in init.rb if they 
  # need plugin directories to be added to the load path, i.e.
  #
  #   plugin.code_paths << 'app/other_classes'
  #
  # Defaults to ["app/controllers", "app/helpers", "app/models", "components"]
  # (see #default_code_paths). NOTE: if you want to set this, you must
  # ensure that the engines plugin is loaded before any plugins which
  # reference this since it's not available before the engines plugin has worked
  # its magic.
  attr_accessor :code_paths
  
  # Plugins can add paths to this attribute in init.rb if they need
  # controllers loaded from additional locations. See also #default_controller_paths, and
  # the caveat surrounding the #code_paths accessor.
  attr_accessor :controller_paths
  
  # The directory in this plugin to mirror into the shared directory
  # under +public+. See Engines.initialize_base_public_directory
  # for more information.
  #
  # Defaults to "assets" (see default_public_directory).
  attr_accessor :public_directory
  
  protected
  
    # The default set of code paths which will be added to $LOAD_PATH
    # and Dependencies.load_paths
    def default_code_paths
      # lib will actually be removed from the load paths when we call
      # uniq! in #inject_into_load_paths, but it's important to keep it
      # around (for the documentation tasks, for instance).
      %w(app/controllers app/helpers app/models components lib)
    end
    
    # The default set of code paths which will be added to the routing system
    def default_controller_paths
      %w(app/controllers components)
    end

    # Attempts to detect the directory to use for public files.
    # If +assets+ exists in the plugin, this will be used. If +assets+ is missing
    # but +public+ is found, +public+ will be used.
    def default_public_directory
      %w(assets public).select { |dir| File.directory?(File.join(root, dir)) }.first || "assets"
    end

  public
  
  # Creates a new Plugin instance, and loads any other data from <tt>about.yml</tt>
  def initialize(name, path)
    @name = name
    @root = path
    
    @code_paths = default_code_paths
    @controller_paths = default_controller_paths
    @public_directory = default_public_directory
    
    load_about_information
  end
  
  # Load the information from <tt>about.yml</tt>. This Hash is then accessible
  # from #about.
  #
  # If <tt>about.yml</tt> includes a "version", this will be assigned
  # automatically into #version.
  def load_about_information
    about_path = File.join(self.root, 'about.yml')
    if File.exist?(about_path)
      @about = YAML.load(File.open(about_path).read)
      @about.stringify_keys!
      @version = @about["version"]
    end
  end
  
  # Load the plugin. Since Rails takes care of evaluating <tt>init.rb</tt> and
  # adding +lib+ to the <tt>$LOAD_PATH</tt>, we don't need to do that here (see
  # Engines::RailsExtensions::RailsInitializer.load_plugins_with_engine_additions).
  # 
  # Here we add controller/helper code to the appropriate load paths (see 
  # #inject_into_load_path) and mirror the plugin assets into the shared public
  # directory (#mirror_public_assets).
  def load
    logger.debug "Plugin '#{name}': starting load."
    
    inject_into_load_path
    mirror_public_assets
    
    logger.debug "Plugin '#{name}': loaded."
  end
  
  # Adds all directories in the +app+ and +lib+ directories within the engine
  # to the three relevant load paths mechanism that Rails might use:
  #
  # * <tt>$LOAD_PATH</tt>
  # * <tt>Dependencies.load_paths</tt>
  # * <tt>ActionController::Routing.controller_paths</tt>
  #
  def inject_into_load_path

    load_path_index = $LOAD_PATH.index(Engines.rails_final_load_path)
    dependency_index = ::Dependencies.load_paths.index(Engines.rails_final_dependency_load_path)
    
    # Add relevant paths under the engine root to the load path
    code_paths.map { |p| File.join(root, p) }.each do |path| 
      if File.directory?(path)
        # Add to the load paths
        $LOAD_PATH.insert(load_path_index + 1, path)
        # Add to the dependency system, for autoloading.
        ::Dependencies.load_paths.insert(dependency_index + 1, path)
      end
    end
    
    # Add controllers to the Routing system specifically. We actually add our paths
    # to the configuration too, since routing is started AFTER plugins are. Plugins
    # which are loaded by engines specifically (i.e. because of the '*' in 
    # +config.plugins+) will need their paths added directly to the routing system, 
    # since at that point it has already been configured.
    controller_paths.map { |p| File.join(root, p) }.each do |path|
      if File.directory?(path)
        ActionController::Routing.controller_paths << path
        Rails.configuration.controller_paths << path
      end
    end

    $LOAD_PATH.uniq!
    ::Dependencies.load_paths.uniq!
    ActionController::Routing.controller_paths.uniq!
    Rails.configuration.controller_paths.uniq!
  end

  # Replicates the subdirectories under the plugins's +assets+ (or +public+) directory into
  # the corresponding public directory. See also Plugin#public_directory for more.
  def mirror_public_assets
  
    begin 
      source = File.join(root, self.public_directory)
      # if there is no public directory, just return after this file
      return if !File.exist?(source)

      logger.debug "Attempting to copy plugin plugin asset files from '#{source}' to '#{Engines.public_directory}'"

      Engines.mirror_files_from(source, File.join(Engines.public_directory, name))
      
    rescue Exception => e
      logger.warn "WARNING: Couldn't create the public file structure for plugin '#{name}'; Error follows:"
      logger.warn e
    end
  end

  # The path to this plugin's public files
  def public_asset_directory
    "#{File.basename(Engines.public_directory)}/#{name}"
  end

  # The directory containing this plugin's migrations (<tt>plugin/db/migrate</tt>)
  def migration_directory
    File.join(self.root, 'db', 'migrate')
  end
  
  # Returns the version number of the latest migration for this plugin. Returns
  # nil if this plugin has no migrations.
  def latest_migration
    migrations = Dir[migration_directory+"/*.rb"]
    return nil if migrations.empty?
    migrations.map { |p| File.basename(p) }.sort.last.match(/0*(\d+)\_/)[1].to_i
  end
  
  # Migrate this plugin to the given version. See Engines::PluginMigrator for more
  # information.   
  def migrate(version = nil)
    Engines::PluginMigrator.migrate_plugin(self, version)
  end  
end