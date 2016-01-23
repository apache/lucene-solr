# This file contains support for the now-deprecated +config+ method that the engines
# plugin provided before version 1.2. Instead of using this, plugin authors are
# now encouraged to create their own Module configuration mechanisms; the 
# +mattr_accessor+ mechanism provided by ActiveSupport is ideal for this:
#
#  module MyPlugin
#    mattr_accessor :config_value
#    self.config_value = "default"
#  end
#
# == Using the deprecated config method
#
# If you require the config method to be present, change your <tt>environment.rb</tt>
# file such that the very top of the file looks like this:
#
#   require File.join(File.dirname(__FILE__), 'boot')
#   require File.join(RAILS_ROOT, "vendor", "plugins", "engines",
#                     "lib", "engines", "deprecated_config_support")
#


# Adds the +config+ and +default_constant+ methods to Module.
#
# *IMPORTANT NOTE* - these methods are deprecated. Only use them when you have no
# other choice. See link:files/lib/engines/deprecated_config_support_rb.html for more
# information.
class Module
  # Defines a constant within a module/class ONLY if that constant does
  # not already exist.
  #
  # This can be used to implement defaults in plugins/engines/libraries, e.g.
  # if a plugin module exists:
  #   module MyPlugin
  #     default_constant :MyDefault, "the_default_value"
  #   end
  #
  # then developers can override this default by defining that constant at
  # some point *before* the module/plugin gets loaded (such as environment.rb)
  def default_constant(name, value)
    if !(name.is_a?(String) or name.is_a?(Symbol))
      raise "Cannot use a #{name.class.name} ['#{name}'] object as a constant name"
    end
    if !self.const_defined?(name)
      self.class_eval("#{name} = #{value.inspect}")
    end
  end
  
  # A mechanism for defining configuration of Modules. With this
  # mechanism, default values for configuration can be provided within shareable
  # code, and the end user can customise the configuration without having to
  # provide all values.
  #
  # Example:
  #
  #  module MyModule
  #    config :param_one, "some value"
  #    config :param_two, 12345
  #  end
  #
  # Those values can now be accessed by the following method
  #
  #   MyModule.config :param_one  
  #     => "some value"
  #   MyModule.config :param_two  
  #     => 12345
  #
  # ... or, if you have overrriden the method 'config'
  #
  #   MyModule::CONFIG[:param_one]  
  #     => "some value"
  #   MyModule::CONFIG[:param_two]  
  #     => 12345
  #
  # Once a value is stored in the configuration, it will not be altered
  # by subsequent assignments, unless a special flag is given:
  #
  #   (later on in your code, most likely in another file)
  #   module MyModule
  #     config :param_one, "another value"
  #     config :param_two, 98765, :force
  #   end
  #
  # The configuration is now:
  #
  #   MyModule.config :param_one  
  #     => "some value" # not changed
  #   MyModule.config :param_two  
  #     => 98765
  #
  # Configuration values can also be given as a Hash:
  #
  #   MyModule.config :param1 => 'value1', :param2 => 'value2'
  #
  # Setting of these values can also be forced:
  #
  #   MyModule.config :param1 => 'value3', :param2 => 'value4', :force => true
  #
  # A value of anything other than false or nil given for the :force key will
  # result in the new values *always* being set.
  def config(*args)
    
    raise "config expects at least one argument" if args.empty?
    
    # extract the arguments
    if args[0].is_a?(Hash)
      override = args[0][:force]
      args[0].delete(:force)
      args[0].each { |key, value| _handle_config(key, value, override)}
    else
      _handle_config(*args)
    end
  end
  
  private
    # Actually set the config values
    def _handle_config(name, value=nil, override=false)
      if !self.const_defined?("CONFIG")
        self.class_eval("CONFIG = {}")
      end
    
      if value != nil
        if override or self::CONFIG[name] == nil
          self::CONFIG[name] = value 
        end
      else
        # if we pass an array of config keys to config(),
        # get the array of values back
        if name.is_a? Array
          name.map { |c| self::CONFIG[c] }
        else
          self::CONFIG[name]
        end
      end      
    end
end