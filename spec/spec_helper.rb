require 'rspec'
require 'rspec'
require 'json'
require 'yaml'
require 'simplecov'

require_relative 'support/bunny_mock'
require_relative 'support/r_solr_mock'

SimpleCov.start do
  add_filter 'spec' # ignore spec files
end

require_relative '../lib/solr_helper'
require_relative '../lib/metadata_helper'
require_relative '../lib/postgres_helper'

require_relative '../lib/ingester'
require_relative '../lib/austalk_ingester'
require_relative '../lib/trove_ingester'

require_relative '../lib/worker'
require_relative '../lib/solr_worker'
require_relative '../lib/upload_worker'
require_relative '../lib/postgres_worker'

require_relative '../lib/sesame_client'
require_relative '../lib/persistent_client'

module SpecHelper

  module ExposePrivate

      def method_missing(method, *args)
          puts method
        send(method, *args)
      end

  end

end
