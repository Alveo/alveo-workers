require 'bunny'
require 'rdf/turtle'
require 'json/ld'

class Ingester

  def initialize(options)
    bunny_client_class = Module.const_get(options[:client_class])
    @bunny_client = bunny_client_class.new(options)
    @exchange_name = options[:exchange]
    @upload_queue_name = options[:upload_queue]
    @sesame_queue_name = options[:sesame_queue]
    @error_logger = Logger.new(options[:error_log])
  end

  def connect
    @bunny_client.start
    @channel = @bunny_client.create_channel
    @exchange = @channel.direct(@exchange_name)
    @upload_queue = add_queue(@upload_queue_name)
    @sesame_queue = add_queue(@sesame_queue_name)
  end

  def close
    @bunny_client.close
  end

  def add_queue(name)
    queue = @channel.queue(name)
    queue.bind(@exchange, routing_key: name)
    queue
  end

  def self.get_rdf_file_paths(dir)
    Dir[File.join(dir, '**', '*')].keep_if { |path|
      (File.file? path) && (File.extname(path) == '.rdf')
    }
  end

  def process_job(collection, file_paths)
    file_paths.each { |file_path|
      begin
        if is_metadata? file_path
          # TODO, file gets read twice, once for metadata, once for sesame
          process_metadata_rdf(file_path)
        end
        add_to_sesame(collection, file_path)
      rescue  Exception => e
        @error_logger.error "#{e.class}: #{e.to_s}"
      end
    }
  end

  def ingest_directory(dir)
    collection = File.basename(dir)
    file_paths = self.class.get_rdf_file_paths(dir)
    process_job(collection, file_paths)
  end

  def process_metadata_rdf(rdf_file)
    graph = RDF::Graph.load(rdf_file, :format => :ttl)
    json_ld = graph.dump(:jsonld)
    properties = {routing_key: @upload_queue.name, headers: {action: 'create'}}
    message = "{\"metadata\":#{json_ld}}"
    @exchange.publish(message, properties)
  end

  def add_to_sesame(collection, rdf_file)
    graph = RDF::Graph.load(rdf_file, :format => :ttl)
    json_ld = graph.dump(:jsonld)
    properties = {routing_key: @sesame_queue.name, headers: {action: 'create'}}
    message = "{\"payload\": #{json_ld}, \"collection\": \"#{collection}\"}"
    @exchange.publish(message, properties)
  end

  def is_metadata?(file_path)
    File.basename(file_path, '.rdf').end_with?('metadata')
  end

end
