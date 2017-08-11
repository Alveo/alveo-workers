require 'bunny'
require 'json'

class TroveIngester


  attr_accessor :ingesting
  attr_reader :record_count

  def initialize(options)
    @ingesting = true
    @options = options
    bunny_client_class = Module.const_get(options[:client_class])
    @bunny_client = bunny_client_class.new(options)
    @exchange_name = options[:exchange]
    @logger = Logger.new(options[:error_log])
    @upload_queue_name = options[:upload_queue]
  end

  def connect
    @bunny_client.start
    @channel = @bunny_client.create_channel
    @exchange = @channel.direct(@exchange_name, durable: true)
    @upload_queue = add_queue(@upload_queue_name)
    monitor_queues
  end

  def monitor_queues
    @monitor_queues = []
    @options[:monitor].each { |queue|
      @monitor_queues << add_queue(queue)
    }
  end

  def monitor_queues_message_count
    message_count = 0
    @monitor_queues.each { |queue|
      message_count += queue.message_count
    }
    message_count
  end

  def close
    @channel.close
    @bunny_client.close
  end

  def add_queue(name)
    queue = @channel.queue(name, durable: true)
    queue.bind(@exchange, routing_key: name)
    queue
  end

  def set_work(work)
    @work = work
  end

  def process()
    @work.each { |trove_chunk|
      process_chunk(trove_chunk)
    }
  end

  def process_chunk(trove_chunk, resume_point=0)
    @record_count = 0
    File.open(trove_chunk, 'r:iso-8859-1').each { |trove_record|
      begin
        if @record_count < resume_point
          @record_count += 1
          next
        end
        trove_fields = JSON.parse(trove_record.encode('utf-8'))
        message = map_to_json_ld(trove_fields, trove_record)
        properties = {routing_key: @upload_queue.name, headers: {action: 'create'}}
        @exchange.publish(message, properties)
        @record_count += 1
        break if !@ingesting
      rescue Exception => e
        # TODO: Error queue instead of log file
        @logger.error "#{e.class}: #{e.to_s}\ninput: #{trove_record}"
      end
    }
  end

  def map_to_json_ld(trove_fields, trove_record)
    # TODO: see if qualified values can be removed, e.g. ausnc:popular
    # TODO: Possibly use type coercion in the context if the generated RDF isn't correct
    #       e.g. "ausnc:audience":{"@type":"http://ns.ausnc.org.au/schemas/ausnc_md_model/audience"},
    %Q({
        "items":[{
        "@context": [
          {
            "ausnc": "http://ns.ausnc.org.au/schemas/ausnc_md_model/",
            "dc": "http://purl.org/dc/terms/",
            "alveo": "http://alveo.edu.au/vocabulary/",
            "olac": "http://www.language-archives.org/OLAC/1.1/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "ausnc:document": {"@type": "@id"},
            "alveo:display_document": {"@type": "@id"},
            "alveo:indexable_document": {"@type": "@id"}
         }],
        "alveo:metadata": {
            "@id": "https://app.alveo.edu.au/catalog/trove/#{trove_fields['id']}",
            "ausnc:audience": "mass_market",
            "ausnc:communication_medium": "newspaper",
            "ausnc:communication_setting": "popular",
            "ausnc:itemwordcount": #{trove_fields['wordCount']},
            "ausnc:mode": "written",
            "ausnc:publication_status": "published",
            "ausnc:state": "#{trove_fields['state'].first}",
            "ausnc:written_mode": "print",
            "ausnc:document": [
              "http://trove.alveo.edu.au/document/#{trove_fields['id']}#Text",
              "http://trove.alveo.edu.au/document/#{trove_fields['id']}#Original"
            ],
            "dcterms:created": "#{trove_fields['date']}",
            "dcterms:identifier": "#{trove_fields['id']}",
            "dcterms:source": #{trove_fields['titleName'].to_json},
            "dcterms:title": #{trove_fields['heading'].to_json},
            "dcterms:isPartOf": "trove",
            "alveo:full_text": #{trove_fields['fulltext'].to_json},
            "alveo:display_document": "http://trove.alveo.edu.au/document/#{trove_fields['id']}#Text",
            "alveo:indexable_document": "http://trove.alveo.edu.au/document/#{trove_fields['id']}#Text",
            "olac:language": "eng"
          },
          "ausnc:document": [{
            "@id":"http://trove.alveo.edu.au/document/#{trove_fields['id']}#Text",
            "@type":"foaf:Document",
            "dcterms:extent": #{trove_fields['fulltext'].size},
            "dcterms:identifier": "#{trove_fields['id']}.txt",
            "dcterms:title": "#{trove_fields['id']}#Text",
            "dcterms:source": "http://trove.alveo.edu.au/document/#{trove_fields['id']}.txt",
            "dcterms:type": "Text",
            "alveo:size": #{trove_fields['fulltext'].bytesize}
          },
          {
            "@id":"http://trove.alveo.edu.au/document/#{trove_fields['id']}#Original",
            "@type":"foaf:Document",
            "dcterms:extent": #{trove_record.size},
            "dcterms:identifier": "#{trove_fields['id']}",
            "dcterms:title": "#{trove_fields['id']}#Original",
            "dcterms:source": "http://trove.alveo.edu.au/document/#{trove_fields['id']}",
            "dcterms:type": "Original",
            "alveo:size": #{trove_record.bytesize}
          }
        ]
        }
    ]})
  end


end
