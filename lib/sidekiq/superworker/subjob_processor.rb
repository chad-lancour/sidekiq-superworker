module Sidekiq
  module Superworker
    class SubjobProcessor
      class << self
        def enqueue(subjob)
          Superworker.debug "#{subjob.to_info}: Trying to enqueue"
          # Only enqueue subjobs that aren't running, complete, etc
          if subjob.status != 'initialized'
            Superworker.debug "#{subjob.to_info}: Enqueue failed, status = #{subjob.status}"
            return
          end

          Superworker.debug "#{subjob.to_info}: Enqueueing"
          # If this is a parallel subjob, enqueue all of its children
          if subjob.subworker_class == 'parallel'
            subjob.update_attribute(:status, 'running')

            Superworker.debug "#{subjob.to_info}: Enqueueing parallel children"
            jids = subjob.children.collect do |child|
              enqueue(child)
            end
            jid = jids.first
          elsif subjob.subworker_class == 'batch'
            subjob.update_attribute(:status, 'running')

            Superworker.debug "#{subjob.to_info}: Enqueueing batch children"
            jids = subjob.children.collect do |child|
              child.update_attribute(:status, 'running')
              enqueue(child.children.first)
            end
            jid = jids.first
          else
            klass = "::#{subjob.subworker_class}".constantize

            # If this is a superworker, mark it as complete, which will queue its children or its next subjob
            if klass.respond_to?(:is_a_superworker?) && klass.is_a_superworker?
              complete(subjob)
            # Otherwise, enqueue it in Sidekiq
            else
              # We need to explicitly set the job's JID, so that the ActiveRecord record can be updated before
              # the job fires off. If the job started first, it could finish before the ActiveRecord update
              # transaction completes, causing a race condition when finding the ActiveRecord record in
              # Processor#complete.
              jid = SecureRandom.hex(12)
              subjob.update_attributes(
                jid: jid,
                status: 'queued'
              )
              enqueue_in_sidekiq(subjob, klass, jid)
            end
          end
          jid
        end

        def enqueue_in_sidekiq(subjob, klass, jid)
          Superworker.debug "#{subjob.to_info}: Enqueueing in Sidekiq"

          # If sidekiq-unique-jobs is being used for this worker, a number of issues arise if the subjob isn't
          # queued, so we'll bypass the unique functionality of the worker while running the subjob.
          is_unique = klass.respond_to?(:sidekiq_options_hash) && !!(klass.sidekiq_options_hash || {})['unique']
          if is_unique
            unique_value = klass.sidekiq_options_hash.delete('unique')
            unique_job_expiration_value = klass.sidekiq_options_hash.delete('unique_job_expiration')
          end

          sidekiq_push(subjob, klass, jid)

          if is_unique
            klass.sidekiq_options_hash['unique'] = unique_value
            klass.sidekiq_options_hash['unique_job_expiration'] = unique_job_expiration_value
          end

          jid
        end

        def complete(subjob)
          Superworker.debug "#{subjob.to_info}: Complete"
          subjob.update_attribute(:status, 'complete')

          # If children are present, enqueue the first one
          children = subjob.children
          if children.present?
            Superworker.debug "#{subjob.to_info}: Enqueueing children"
            enqueue(children.first)
            return
          # Otherwise, set this as having its descendants complete
          else
            descendants_are_complete(subjob)
          end
        end

        def error(subjob, worker, item, exception)
          Superworker.debug "#{subjob.to_info}: Error"
          subjob.update_attribute(:status, 'failed')
          SuperjobProcessor.error(subjob.superjob_id, worker, item, exception)
        end

        protected

        def descendants_are_complete(subjob)
          Superworker.debug "#{subjob.to_info}: Descendants are complete"
          subjob.update_attribute(:descendants_are_complete, true)

          if subjob.subworker_class == 'batch_child' || subjob.subworker_class == 'batch'
            complete(subjob)
          end

          parent = subjob.parent
          is_child_of_parallel = parent && parent.subworker_class == 'parallel'

          # If a parent exists, check whether this subjob's siblings are all complete
          if parent
            siblings_descendants_are_complete = parent.children.all? { |child| child.descendants_are_complete }
            if siblings_descendants_are_complete
              Superworker.debug "#{subjob.to_info}: Parent (#{parent.to_info}) is complete"
              descendants_are_complete(parent)
              parent.update_attribute(:status, 'complete') if is_child_of_parallel
            end
          end

          unless is_child_of_parallel
            # If a next subjob is present, enqueue it
            next_subjob = subjob.next
            if next_subjob
              # OK this is where the problem is, was. If a superworker has sequential groups of parallel workers, that all finish very quickly and generally at the same time - one or more of them will basically end up here at the same time, grabbing the same next sub job. That next sub job would be the next sequential parallel group. What would have happened is the next parallel group would get sent to sidekiq multiple times!
              # The whole approach of this fix was to minimally change code. Overall the fix is a band-aid.
              # The quick fix was to lock the next subjob row, update it to an interim status of 'queued' to ensure multiple complete subjobs would not pick it up. Then pass the original next_subjob with the original status of initialized - because I did not want to change the enqueue logic that only processes initialized.
              # During debugging and testing, I implemented optimistic locking on the sidekiq_superworker_subjobs table by adding lock_version col and setting ActiveRecord::Base.lock_optimistically = true. The result overall, is that there are several places where the code is actually updating subjob records that are stale. Though most of the time it's just the updated_at field that is stale.
              # !! I think the proper fix would involve thoroughly decoupling the subjob complete process from the enqueue process.
              #   1. refactor code to ensure that each subjob is only updating it's own subjob record. Don't touch parents, and relatives, and children, etc.
              #   2. create a seperate process in it's own thread that is responsible for monitoring parallel, batch and superworker job states. this process would determine what's next, properly update subjob records, and sending subjobs to sidekiq. overall this might entail using a proper queuing mechanism designed for highly parallel environments and could better support multi-node sidekiq clients...

              enqueue_next_job = false
              next_subjob.with_lock do
                # ensure we are only fetching next subjobs that are in initialized state.
                # !! skipping subjob.next method because
                #   a. we don't want the relatives where clause ( self.class.where(superjob_id: superjob_id) ) because it causes mysql to scan the superjob_id index and in the process it will eagerly lock all subjob rows for this superjob! PLUS subjob.id is primary key - just use that for next subjob id.
                #   b. we want to ensure that we only get next subjobs that are in state initliazed.
                locked_next_subjob = Sidekiq::Superworker::Subjob.lock.where( { id: next_subjob.id, status: 'initialized' } ).first
                if locked_next_subjob
                  locked_next_subjob.update_attributes( status: 'queued' )
                  Superworker.debug "#{subjob.to_info}: Enqueueing Next Subjob ID #{next_subjob.id}, New DB Status = #{locked_next_subjob.status}, Enqueueing with Original Status = #{next_subjob.status}"
                  enqueue_next_job = true
                else
                  Superworker.debug "#{subjob.to_info}: NOT Enqueueing Next Subjob ID #{next_subjob.id}. Multiple Subjobs attempting to enqueue."
                  enqueue_next_job = false
                end
              end
              enqueue(next_subjob) if enqueue_next_job
              return
            end

            # If there isn't a parent, then, this is the final subjob of the superjob
            unless parent
              Superworker.debug "#{subjob.to_info}: Superjob is complete"
              SuperjobProcessor.complete(subjob.superjob_id)
            end
          end
        end

        def sidekiq_push(subjob, klass, jid)
          # This is akin to perform_async, but it allows us to explicitly set the JID
          item = sidekiq_item(subjob, klass, jid)
          Sidekiq::Client.push(item)
        end

        def sidekiq_item(subjob, klass, jid)
          item = { 'class' => klass, 'args' => subjob.arg_values, 'jid' => jid }
          if subjob.meta && subjob.meta[:sidekiq]
            item.merge!(subjob.meta[:sidekiq].stringify_keys)
          end
          item
        end
      end
    end
  end
end
