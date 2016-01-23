begin
  silence_warnings { require 'rails/version' } # it may already be loaded
  unless Rails::VERSION::MAJOR >= 1 && Rails::VERSION::MINOR >= 2
    raise "This version of the engines plugin requires Rails 1.2 or later!"
  end
end

# First, require the engines module & core methods
require "engines"

# Load this before we get actually start engines
require "engines/rails_extensions/rails_initializer"

# Start the engines mechanism.
Engines.init(config, self)

# Now that we've defined the engines module, load up any extensions
[:rails,
 :rails_initializer,
 :dependencies,
 :active_record,
 :migrations,
 :templates,
 :public_asset_helpers,
 :routing
].each do |f|
  require "engines/rails_extensions/#{f}"
end

# Load the testing extensions, if we are in the test environment.
require "engines/testing" if RAILS_ENV == "test"

# Load the Rails::Info module so that plugins can insert information into it.
begin
  require 'rails/info'
rescue Exception
  # If this file can't be loaded, it's probably because we're running in an
  # environment where Rails' builtins aren't yet in the load path.
  # For the moment, just ignore this. See Ticket #261
end