# frozen_string_literal: true

require 'sidekiq/metrics/adapter/base'
require 'sidekiq/metrics/adapter/bigquery/version'

module Sidekiq
  module Metrics
    module Adapter
      class Bigquery < Base
        def initialize(bigquery, dataset, table_id = nil, mapping = {
          status: 'status',
          queue: 'queue',
          class: 'class',
          jid: 'jid',
          enqueued_at: 'enqueued_at',
          started_at: 'started_at',
          finished_at: 'finished_at'
        }, retry_count: 0)
          @bigquery = bigquery
          @table = @bigquery.dataset(dataset).table(table_id)
          @mapping = mapping
          @retry_count = result
        end

        def write(worker_status)
          try = 0
          begin
            break false if try > retry_count
            result = @table.insert([worker_status])
            try += 1
          end until result.success?
        end

        private

        def mapped_row(worker_status)
          {}.tap do |row|
            @mapping.each do |from, to|
              row[to] = worker_status[form]
            end
          end
        end
      end
    end
  end
end
