# Be sure to restart your web server when you modify this file.

# Uncomment below to force Rails into production mode when 
# you don't control web/app server and can't set it the proper way
# ENV['RAILS_ENV'] ||= 'production'

# Specifies gem version of Rails to use when vendor/rails is not present
RAILS_GEM_VERSION = '1.2.2' unless defined? RAILS_GEM_VERSION

# Bootstrap the Rails environment, frameworks, and default configuration
require File.join(File.dirname(__FILE__), 'boot')

Rails::Initializer.run do |config|
  # Settings in config/environments/* take precedence over those specified here
  
  # Skip frameworks you're not going to use (only works if using vendor/rails)
  # config.frameworks -= [ :action_web_service, :action_mailer ]

  # Only load the plugins named here, by default all plugins in vendor/plugins are loaded
  # config.plugins = %W( exception_notification ssl_requirement )

  # Add additional load paths for your own custom dirs
  # config.load_paths += %W( #{RAILS_ROOT}/extras )

  # Force all environments to use the same logger level 
  # (by default production uses :info, the others :debug)
  # config.log_level = :debug

  # Use the database for sessions instead of the file system
  # (create the session table with 'rake db:sessions:create')
  # config.action_controller.session_store = :active_record_store

  # Use SQL instead of Active Record's schema dumper when creating the test database.
  # This is necessary if your schema can't be completely dumped by the schema dumper, 
  # like if you have constraints or database-specific column types
  # config.active_record.schema_format = :sql

  # Activate observers that should always be running
  # config.active_record.observers = :cacher, :garbage_collector

  # Make Active Record use UTC-base instead of local time
  # config.active_record.default_timezone = :utc
  
  # See Rails::Configuration for more options
end

# Add new inflection rules using the following format 
# (all these examples are active by default):
# Inflector.inflections do |inflect|
#   inflect.plural /^(ox)$/i, '\1en'
#   inflect.singular /^(ox)en/i, '\1'
#   inflect.irregular 'person', 'people'
#   inflect.uncountable %w( fish sheep )
# end

# Include your application configuration below
# $KCODE = 'UTF8' # Rails 1.2 supposedly sets this automatically

require 'solr'

solr_environments = {
  # facets: default, all *_facet fields are considered facet fields
  # title: default, :title_text is title field
  # timeline: default, no timeline support without knowing the field(s) to use
  
  :development => {
  },

  :delicious => {
#    :image_url => Proc.new {|d| "http://images.amazon.com/images/P/#{d[:asin_text]}.01.MZZZZZZZ"},
    :timeline_dates => :published_year_facet
  },

  :tang => {
  },
  
  :marc => {
    :timeline_dates => :year_facet
  },
  
  # TODO: :uva could inherit :marc settings, only overriding the template for VIRGO links
  :uva => {
    :timeline_dates => :year_facet
  },
}
SOLR_ENV = ENV["SOLR_ENV"] || "development"
SOLR_CONFIG = solr_environments[SOLR_ENV.to_sym]
puts "#{SOLR_ENV}: SOLR_CONFIG = #{SOLR_CONFIG.inspect}"
solr_url = SOLR_CONFIG[:solr_url] || "http://localhost:8983/solr"
SOLR = Solr::Connection.new(solr_url)
