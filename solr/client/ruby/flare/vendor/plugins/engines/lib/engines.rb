# This file contains the Engines module, which holds most of the logic regarding
# the startup and management of plugins. See Engines for more details.
#
# The engines plugin adds to Rails' existing behaviour by producing the +Rails.plugins+
# PluginList, a list of all loaded plugins in a form which can be easily queried
# and manipulated. Each instance of Plugin has methods which are used to enhance
# their behaviour, including mirroring public assets, add controllers, helpers
# and views and even migration.
#
# = init.rb
# 
# When the engines plugin loads, it first includes the 
# Engines::RailsExtensions::RailsInitializer module into Rails::Initializer,
# overriding a number of the methods used to load plugins.
#
# Once this is loaded, Engines.init is called to prepare the application
# and create the relevant new datastructures (including <tt>Rails.plugins</tt>).
#
# Finally, each of the extension modules from Engines::RailsExtensionsis 
# loaded and included into the relevant Rails classes and modules, enhancing 
# their behaviour to work better with files from plugins.

require "engines/plugin_list"
require "engines/plugin"

# TODO: define a better logger.
def logger
  RAILS_DEFAULT_LOGGER
end

# The Engines module contains most of the methods used during the enhanced
# startup of Rails plugins.
#
# When the Engines plugin loads (its <tt>init.rb</tt> file is evaluated), the
# Engines.init method is called. This kickstarts the plugins hooks into 
# the initialization process.
#
# == Parameters
#
# The Engines module has a number of public configuration parameters:
#
# [+public_directory+]  The directory into which plugin assets should be
#                       mirrored. Defaults to <tt>RAILS_ROOT/public/plugin_assets</tt>.
# [+schema_info_table+] The table to use when storing plugin migration 
#                       version information. Defaults to +plugin_schema_info+.
# [+rails_initializer+] A reference of the Rails initializer instance that
#                       was used to startup Rails. This is often useful
#                       when working with the startup process; see
#                       Engines::RailsExtensions::RailsInitializer for more
#                       information
#
# Additionally, there are a few flags which control the behaviour of
# some of the features the engines plugin adds to Rails:
#
# [+disable_application_view_loading+] A boolean flag determining whether
#                                      or not views should be loaded from 
#                                      the main <tt>app/views</tt> directory.
#                                      Defaults to false; probably only 
#                                      useful when testing your plugin.
# [+disable_application_code_loading+] A boolean flag determining whether
#                                      or not to load controllers/helpers 
#                                      from the main +app+ directory,
#                                      if corresponding code exists within 
#                                      a plugin. Defaults to false; again, 
#                                      probably only useful when testing 
#                                      your plugin.
# [+disable_code_mixing+] A boolean flag indicating whether all plugin
#                         copies of a particular controller/helper should 
#                         be loaded and allowed to override each other, 
#                         or if the first matching file should be loaded 
#                         instead. Defaults to false.
#
module Engines
  # The name of the public directory to mirror public engine assets into.
  # Defaults to <tt>RAILS_ROOT/public/plugin_assets</tt>.
  mattr_accessor :public_directory
  self.public_directory = File.join(RAILS_ROOT, 'public', 'plugin_assets')

  # The table in which to store plugin schema information. Defaults to
  # "plugin_schema_info".
  mattr_accessor :schema_info_table
  self.schema_info_table = "plugin_schema_info"

  # A reference to the current Rails::Initializer instance
  mattr_accessor :rails_initializer
  
  
  #--
  # These attributes control the behaviour of the engines extensions
  #++
  
  # Set this to true if views should *only* be loaded from plugins
  mattr_accessor :disable_application_view_loading
  self.disable_application_view_loading = false
  
  # Set this to true if controller/helper code shouldn't be loaded 
  # from the application
  mattr_accessor :disable_application_code_loading
  self.disable_application_code_loading = false
  
  # Set this ti true if code should not be mixed (i.e. it will be loaded
  # from the first valid path on $LOAD_PATH)
  mattr_accessor :disable_code_mixing
  self.disable_code_mixing = false
  
  
  private

  # A memo of the bottom of Rails' default load path
  mattr_accessor :rails_final_load_path
  # A memo of the bottom of Rails Dependencies load path
  mattr_accessor :rails_final_dependency_load_path
  
  public
  
  # Initializes the engines plugin and prepares Rails to start loading
  # plugins using engines extensions. Within this method:
  #
  # 1. Copies of the Rails configuration and initializer are stored;
  # 2. The Rails.plugins PluginList instance is created;
  # 3. Any plugins which were loaded before the engines plugin are given 
  #    the engines treatment via #enhance_loaded_plugins.
  # 4. The base public directory (into which plugin assets are mirrored)
  #    is created, if necessary - #initialize_base_public_directory
  # 5. <tt>config.plugins</tt> is checked to see if a wildcard was present -
  #    #check_for_star_wildcard
  #
  def self.init(rails_configuration, rails_initializer)
    # First, determine if we're running in legacy mode
    @legacy_support = self.const_defined?(:LegacySupport) && LegacySupport

    # Store some information about the plugin subsystem
    Rails.configuration = rails_configuration

    # We need a hook into this so we can get freaky with the plugin loading itself
    self.rails_initializer = rails_initializer
    
    @load_all_plugins = false    
    
    store_load_path_markers
    
    Rails.plugins ||= PluginList.new
    enhance_loaded_plugins # including this one, as it happens.

    initialize_base_public_directory
    
    check_for_star_wildcard
    
    logger.debug "engines has started."
  end

  # You can enable legacy support by defining the LegacySupport constant
  # in the Engines module before Rails loads, i.e. at the *top* of environment.rb,
  # add:
  # 
  #   module Engines
  #     LegacySupport = true
  #   end
  #
  # Legacy Support doesn't actually do anything at the moment. If necessary
  # we may support older-style 'engines' using this flag.
  def self.legacy_support?
    @legacy_support
  end

  # A reference to the currently-loading/loaded plugin. This is present to support
  # legacy engines; it's preferred to use Rails.plugins[name] in your plugin's
  # init.rb file in order to get your Plugin instance.
  def self.current
    Rails.plugins.last
  end

  # This is set to true if a "*" widlcard is present at the end of
  # the config.plugins array.  
  def self.load_all_plugins?
    @load_all_plugins
  end

  # Stores a record of the last paths which Rails added to each of the load path
  # attributes ($LOAD_PATH, Dependencies.load_paths and 
  # ActionController::Routing.controller_paths) that influence how code is loaded
  # We need this to ensure that we place our additions to the load path *after*
  # all Rails' defaults
  def self.store_load_path_markers
    self.rails_final_load_path = $LOAD_PATH.last
    logger.debug "Rails final load path: #{self.rails_final_load_path}"
    self.rails_final_dependency_load_path = ::Dependencies.load_paths.last
    logger.debug "Rails final dependency load path: #{self.rails_final_dependency_load_path}"
  end
  
  # Create Plugin instances for plugins loaded before the engines plugin was.
  # Once a Plugin instance is created, the Plugin#load method is then called
  # to fully load the plugin. See Plugin#load for more details about how a
  # plugin is started once engines is involved.
  def self.enhance_loaded_plugins
    Engines.rails_initializer.loaded_plugins.each do |name|
      plugin_path = File.join(self.find_plugin_path(name), name)
      unless Rails.plugins[name]
        plugin = Plugin.new(name, plugin_path)
        logger.debug "enginizing plugin: #{plugin.name} from #{plugin_path}"
        plugin.load # injects the extra directories into the load path, and mirrors public files
        Rails.plugins << plugin
      end
    end
    logger.debug "plugins is now: #{Rails.plugins.map { |p| p.name }.join(", ")}"
  end  
  
  # Ensure that the plugin asset subdirectory of RAILS_ROOT/public exists, and
  # that we've added a little warning message to instruct developers not to mess with
  # the files inside, since they're automatically generated.
  def self.initialize_base_public_directory
    if !File.exist?(self.public_directory)
      # create the public/engines directory, with a warning message in it.
      logger.debug "Creating public engine files directory '#{self.public_directory}'"
      FileUtils.mkdir(self.public_directory)
      message = %{Files in this directory are automatically generated from your Rails Engines.
They are copied from the 'public' directories of each engine into this directory
each time Rails starts (server, console... any time 'start_engine' is called).
Any edits you make will NOT persist across the next server restart; instead you
should edit the files within the <plugin_name>/assets/ directory itself.}
      target = File.join(public_directory, "README")
      File.open(target, 'w') { |f| f.puts(message) } unless File.exist?(target)
    end
  end
  
  # Check for a "*" at the end of the plugins list; if one is found, note that
  # we should load all other plugins once Rails has finished initializing, and
  # remove the "*".
  def self.check_for_star_wildcard
    if Rails.configuration.plugins && Rails.configuration.plugins.last == "*"
      Rails.configuration.plugins.pop
      @load_all_plugins = true
    end 
  end


  #-
  # The following code is called once all plugins are loaded, and Rails is almost
  # finished initialization
  #+

  # Once the Rails Initializer has finished, the engines plugin takes over
  # and performs any post-processing tasks it may have, including:
  #
  # * Loading any remaining plugins if config.plugins ended with a '*'.
  # * Updating Rails::Info with version information, if possible.
  #
  def self.after_initialize
    if self.load_all_plugins?
      logger.debug "loading remaining plugins from #{Rails.configuration.plugin_paths.inspect}"
      # this will actually try to load ALL plugins again, but any that have already 
      # been loaded will be ignored.
      rails_initializer.load_all_plugins
      update_rails_info_with_loaded_plugins
    end
  end
  
  # Updates Rails::Info with the list of loaded plugins, and version information for
  # each plugin. This information is then available via script/about, or through
  # the builtin rails_info controller.
  def self.update_rails_info_with_loaded_plugins
    if defined?(Rails::Info) # since it may not be available by default in some environments... 
                             # don't do anything if it's not there.
      Rails::Info.property("Loaded plugins") { Rails.plugins.map { |p| p.name }.join(", ") }
      Rails.plugins.each do |plugin|
        Rails::Info.property("#{plugin.name} version") { plugin.version.blank? ? "(unknown)" : plugin.version }
      end
    end      
  end
  
  #-
  # helper methods to find and deal with plugin paths and names
  #+
  
  # Returns the path within +Rails.configuration.plugin_paths+ which includes
  # a plugin with the given name.
  def self.find_plugin_path(name)
    Rails.configuration.plugin_paths.find do |path|
      File.exist?(File.join(path, name.to_s))
    end    
  end
  
  # Returns the name for the plugin at the given path.
  # (Note this method also appears in Rails::Initializer extensions)
  def self.plugin_name(path)
    File.basename(path)
  end
  
  # A general purpose method to mirror a directory (+source+) into a destination
  # directory, including all files and subdirectories. Files will not be mirrored
  # if they are identical already (checked via FileUtils#identical?).
  def self.mirror_files_from(source, destination)
    return unless File.directory?(source)
    
    # TODO: use Rake::FileList#pathmap?
    
    source_files = Dir[source + "/**/*"]
    source_dirs = source_files.select { |d| File.directory?(d) }
    source_files -= source_dirs  
    
    source_dirs.each do |dir|
      # strip down these paths so we have simple, relative paths we can
      # add to the destination
      target_dir = File.join(destination, dir.gsub(source, ''))
      begin        
        FileUtils.mkdir_p(target_dir)
      rescue Exception => e
        raise "Could not create directory #{target_dir}: \n" + e
      end
    end

    source_files.each do |file|
      begin
        target = File.join(destination, file.gsub(source, ''))
        unless File.exist?(target) && FileUtils.identical?(file, target)
          FileUtils.cp(file, target)
        end 
      rescue Exception => e
        raise "Could not copy #{file} to #{target}: \n" + e 
      end
    end  
  end
end