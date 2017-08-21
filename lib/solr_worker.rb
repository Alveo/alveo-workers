require 'rsolr'
require 'easy_logging'
require_relative 'worker'
require_relative 'solr_helper'

class SolrWorker < Worker

  # TODO:
  #   - MonkeyPatch persistent HTTP connections

  include SolrHelper
  include EasyLogging

  def initialize(options)
    # EasyLogging.log_destination = options[:logger]
    logger.debug "#initialize"

    rabbitmq_options = options[:rabbitmq]
    super(rabbitmq_options)
    solr_client_class = Module.const_get(options[:client_class])
    @solr_client = solr_client_class.connect(url: options[:url])
    @batch_options = options[:batch].freeze
    if @batch_options[:enabled]
      @batch = []
      @batch_mutex = Mutex.new
    end


  end

  # TODO: this could possible be refactored to super class
  def start_batch_monitor
    logger.debug "#start_batch_monitor"

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
  end

  def process_message(headers, message)
    logger.debug "#process_message"

    if headers['action'] == 'create'
      document = create_solr_document(message)
      if @batch_options[:enabled]
        batch_create(document)
      else
        add_documents(document)
      end
    end
  end


  def commit_batch
    logger.debug "#commit_batch"

    @batch_mutex.synchronize {
      if !@batch.empty?
        add_documents(@batch)
        @batch.clear
      end
    }
  end

  def add_documents(documents)
    logger.debug "#add_documents: documents[#{documents.size}]"

    response = @solr_client.add(documents)

    logger.debug "add_documents: response[#{response}]"

    status = response['responseHeader']['status']
    if status != 0
      msg = "Solr returned an unexpected status: #{status}"
      logger.error "add_documents: #{msg}"

      raise msg
    end

    logger.info "add_documents: response status[#{status}]"

    @solr_client.commit

    logger.info "add_documents: add documents to RSolr successfully done with response[#{status}]"
  end

  def batch_create(document)
    logger.debug "#batch_create: document[#{document.size}]"

    @batch_mutex.synchronize {
      @batch << document
    }

    logger.debug "batch_create: current document(s)/batch size[#{@batch.size}/#{@batch_options[:size]}]"
    if (@batch.size >= @batch_options[:size])
      commit_batch
    end
  end

end