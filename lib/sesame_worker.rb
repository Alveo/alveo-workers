require 'rdf'
require 'rdf/turtle'

require_relative 'worker'
require_relative 'sesame_client'
require_relative 'sesame_helper'

class SesameWorker < Worker

  include SesameHelper

  def initialize(options)
    rabbitmq_options = options[:rabbitmq]
    super(rabbitmq_options)
    sesame_client_class = Module.const_get(options[:client_class])
    @sesame_client = sesame_client_class.new(options)
    @batch_options = options[:batch].freeze
    @file_output_dir = options[:file_output_dir]
    if @batch_options[:enabled]
      @batch = RDF::Repository.new
      @batch_mutex = Mutex.new
    end
  end

  def start_batch_monitor
    @batch_monitor = Thread.new {
      loop {
        sleep @batch_options[:timeout]
        commit_batch
      }
    }
  end

  def start
    super
    if @batch_options[:enabled]
      start_batch_monitor
    end
  end

  def stop
    super
    if @batch_options[:enabled]
      @batch_monitor.kill
      commit_batch
    end
  end

  def close
    super
    @sesame_client.close
  end

  def connect
    super
  end

  def process_message(headers, message)
    if headers['action'] == 'create'
      collection = headers['collection']
      #collection = message['alveo:metadata']['dcterms:isPartOf']
      rdf_graph = create_rdf_graph(message)
      if @batch_options[:enabled]
        batch_create(collection, rdf_graph)
      else
        insert_statements(collection, rdf_graph)
      end
    end
  end

  def commit_batch
    @batch_mutex.synchronize {
      if !@batch.empty?
        n3_string = RDF::NTriples::Writer.dump(@batch, nil, :encoding => Encoding::ASCII)
        if @file_output_dir
          filename = "trove_#{Time.now.strftime('%Y%m%d_%H%M%S_%6N')}.rdf"
          path = File.join(@file_output_dir, filename)
          File.open(path, 'w') { |output_file|
            output_file.write(n3_string)
          }
        else
          @sesame_client.batch_insert_statements(@collection, n3_string)
        end
        @batch.clear!
      end
    }
  end

  def insert_statements(collection, rdf_graph)
    # TODO: this is inconsistant with the batch interface
    @sesame_client.insert_statements(@collection, message['payload'])
  end

  def batch_create(collection, rdf_graph)
    # TODO: This is flawed - if multiple collections are processed
    # at the same time, it can result in statements being inserted
    # into the wrong collection. It may be better to maintain a hash
    # of batches keyed on collections, e.g. {'collection' => []}
    @collection = collection
    @batch_mutex.synchronize {
      @batch << rdf_graph
    }
    if (@batch.size >= @batch_options[:size])
      commit_batch
    end
  end

end
