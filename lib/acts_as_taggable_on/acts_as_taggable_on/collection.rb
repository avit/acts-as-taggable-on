module ActsAsTaggableOn::Taggable
  module Collection
    def self.included(base)
      base.send :include, ActsAsTaggableOn::Taggable::Collection::InstanceMethods
      base.extend ActsAsTaggableOn::Taggable::Collection::ClassMethods
      base.initialize_acts_as_taggable_on_collection
    end

    module ClassMethods
      def initialize_acts_as_taggable_on_collection
        tag_types.map(&:to_s).each do |tag_type|
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def self.#{tag_type.singularize}_counts(options={})
              tag_counts_on('#{tag_type}', options)
            end

            def #{tag_type.singularize}_counts(options = {})
              tag_counts_on('#{tag_type}', options)
            end

            def top_#{tag_type}(limit = 10)
              tag_counts_on('#{tag_type}', :order => 'count desc', :limit => limit.to_i)
            end

            def self.top_#{tag_type}(limit = 10)
              tag_counts_on('#{tag_type}', :order => 'count desc', :limit => limit.to_i)
            end
          RUBY
        end
      end

      def acts_as_taggable_on(*args)
        super(*args)
        initialize_acts_as_taggable_on_collection
      end

      def tag_counts_on(context, options = {})
        all_tag_counts(options.merge(:on => context.to_s))
      end

      def tags_on(context, options = {})
        all_tags(options.merge(:on => context.to_s))
      end

      ##
      # Calculate the tag names.
      # To be used when you don't need tag counts and want to avoid the taggable joins.
      #
      # @param [Hash] options Options:
      #                       * :start_at   - Restrict the tags to those created after a certain time
      #                       * :end_at     - Restrict the tags to those created before a certain time
      #                       * :conditions - A piece of SQL conditions to add to the query. Note we don't join the taggable objects for performance reasons.
      #                       * :limit      - The maximum number of tags to return
      #                       * :order      - A piece of SQL to order by. Eg 'tags.count desc' or 'taggings.created_at desc'
      #                       * :on         - Scope the find to only include a certain context
      def all_tags(options = {})
        options.assert_valid_keys :start_at, :end_at, :conditions, :order, :limit, :on

        context = options[:on].to_s if options[:on]

        tagging_table       = arel_taggings_table(context: context)
        tag_table           = arel_tags_table(context: context)

        after_start = tagging_table[:created_at].gteq(options.delete(:start_at)) if options[:start_at]
        before_end  = tagging_table[:created_at].lteq(options.delete(:end_at))   if options[:end_at]

        taggable_table_name = ["taggable", context, table_name].compact.join(?_)

        taggable_scope      = unscope(:select).select(arel_table[primary_key])

        for_taggable        = tagging_table[:taggable_id].in(taggable_scope.arel)
                         .and tagging_table[:taggable_type].eq(base_class.name)

        in_context          = tagging_table[:context].eq(context) if context

        tag_key             = tagging_table[:tag_id].eq(tag_table['id'])

        tag_conditions      = Array(options[:conditions]).compact

        tagging_conditions  = [for_taggable, in_context, after_start, before_end].compact

        tagging_scope = ActsAsTaggableOn::Tagging
                .where(tagging_conditions.reduce(&:and))

        ActsAsTaggableOn::Tag
                .select(tag_table[Arel.star]).uniq
                .from(tag_table)
                .joins(tag_table.join(tagging_table).on(tag_key).join_sources)
                .merge(tagging_scope)
                .where(tag_conditions.reduce(&:and))
                .order(options[:order])
                .limit(options[:limit])
      end

      ##
      # Calculate the tag counts for all tags.
      #
      # @param [Hash] options Options:
      #                       * :start_at   - Restrict the tags to those created after a certain time
      #                       * :end_at     - Restrict the tags to those created before a certain time
      #                       * :conditions - A piece of SQL conditions to add to the query
      #                       * :limit      - The maximum number of tags to return
      #                       * :order      - A piece of SQL to order by. Eg 'tags.count desc' or 'taggings.created_at desc'
      #                       * :at_least   - Exclude tags with a frequency less than the given value
      #                       * :at_most    - Exclude tags with a frequency greater than the given value
      #                       * :on         - Scope the find to only include a certain context
      def all_tag_counts(options = {})
        options.assert_valid_keys :start_at, :end_at, :conditions, :at_least, :at_most, :order, :limit, :on, :id
        context = options[:on]

        tag_table     = arel_tags_table
        counts_table  = arel_taggings_table(context: context, for: "counts")
        tag_id        = counts_table[:tag_id]
        tags_count    = tag_id.count.as('tags_count')

        group_count   = counts_table[:tags_count].as('count')

        tagging_key   = counts_table[:tag_id].eq(tag_table[:id])

        at_least      = tag_id.count.gteq([1, options[:at_least].to_i].max)
        at_most       = tag_id.count.lteq(options[:at_most]) if options[:at_most]
        in_context    = counts_table[:context].eq(context) if context

        count_conditions = [in_context].compact.reduce(&:and)

        counts_scope  = ActsAsTaggableOn::Tagging
                          .select([tag_id, tags_count])
                          .from(counts_table)
                          .group(tag_id)
                          .where(count_conditions)
                          .having(at_least, at_most)

        if options[:id]
          counts_scope = counts_scope.where(counts_table[:taggable_id].eq(options[:id]))
        end

        counts_subquery = arel_table.join(counts_scope.as(counts_table.table_alias)).on(tagging_key)

        scope = all_tags.joins(counts_subquery.join_sources)
                .select(tag_table[Arel.star], group_count)
                .order(options[:order])
                .limit(options[:limit])
      end
    end

    module InstanceMethods
      def tag_counts_on(context, options={})
        self.class.tag_counts_on(context, options.merge(:id => id))
      end
    end
  end
end
