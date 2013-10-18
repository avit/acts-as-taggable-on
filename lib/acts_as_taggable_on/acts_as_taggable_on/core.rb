module ActsAsTaggableOn::Taggable
  module Core
    def self.included(base)
      base.send :include, ActsAsTaggableOn::Taggable::Core::InstanceMethods
      base.extend ActsAsTaggableOn::Taggable::Core::ClassMethods

      base.class_eval do
        attr_writer :custom_contexts
        after_save :save_tags
      end

      base.initialize_acts_as_taggable_on_core
    end

    module ClassMethods

      def initialize_acts_as_taggable_on_core
        include taggable_mixin
        tag_types.map(&:to_s).each do |tags_type|
          tag_type         = tags_type.to_s.singularize
          context_taggings = "#{tag_type}_taggings".to_sym
          context_tags     = tags_type.to_sym
          taggings_order   = (preserve_tag_order? ? "#{ActsAsTaggableOn::Tagging.table_name}.id" : [])

          class_eval do
            # when preserving tag order, include order option so that for a 'tags' context
            # the associations tag_taggings & tags are always returned in created order
            has_many_with_compatibility context_taggings, :as => :taggable,
                                        :dependent => :destroy,
                                        :class_name => "ActsAsTaggableOn::Tagging",
                                        :order => taggings_order,
                                        :conditions => ["#{ActsAsTaggableOn::Tagging.table_name}.context = (?)", tags_type],
                                        :include => :tag

            has_many_with_compatibility context_tags, :through => context_taggings,
                                        :source => :tag,
                                        :class_name => "ActsAsTaggableOn::Tag",
                                        :order => taggings_order

          end

          taggable_mixin.class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def #{tag_type}_list
              tag_list_on('#{tags_type}')
            end

            def #{tag_type}_list=(new_tags)
              set_tag_list_on('#{tags_type}', new_tags)
            end

            def all_#{tags_type}_list
              all_tags_list_on('#{tags_type}')
            end
          RUBY
        end
      end

      def taggable_on(preserve_tag_order, *tag_types)
        super(preserve_tag_order, *tag_types)
        initialize_acts_as_taggable_on_core
      end

      # all column names are necessary for PostgreSQL group clause
      def grouped_column_names_for(object)
        object.column_names.map { |column| "#{object.table_name}.#{column}" }.join(", ")
      end

      def arel_taggings_table(options = {})
        tagging      = ActsAsTaggableOn::Tagging
        context_name = options[:context].to_s.underscore.pluralize if options[:context]
        alias_parts  = [context_name, tagging.table_name, base_class.table_name, "join", options[:for]]
        Arel::Table.new(tagging.table_name, engine: tagging, as: alias_parts.compact.join(?_))
      end

      def arel_tags_table(options = {})
        tag          = ActsAsTaggableOn::Tag
        context_name = [options[:context].to_s.underscore, "tags"].compact.uniq.join(?_) if options[:context]
        Arel::Table.new(tag.table_name, engine: tag, as: context_name)
      end

      def joins_taggings(tags, options)
        context = options[:on]
        owner   = options[:owned_by]
        exclude = options[:exclude]

        tag_ids = tags.map(&:id)
        tag_ids = [tag_ids] if options[:any]

        matching_tags = tag_ids.map do |tag_id|
          tagging_table   = arel_taggings_table(context: context, for: tag_id)

          for_tag         = tagging_table[:tag_id].public_send(exclude ? :not_in : :in, tag_id)

          for_context     = tagging_table[:context].eq(context) if context

          for_taggable    = tagging_table[:taggable_id].eq(arel_table[primary_key])
                       .and tagging_table[:taggable_type].eq(base_class.name)
                       # TODO: if current model is STI descendant, add type checking to the join condition
                       # << " AND #{table_name}.#{inheritance_column} = '#{name}'" unless descends_from_active_record?

          for_owner       = tagging_table[:tagger_id].eq(owner.id)
                       .and tagging_table[:tagger_type].eq(owner.class.base_class.name) if owner

          tag_conditions  = [for_taggable, for_tag, for_context, for_owner].compact.reduce(&:and)

          arel_table.join(tagging_table).on(tag_conditions).join_sources
        end

        matching_tags.reduce(self, &:joins)
      end

      def without_taggings(tags, options={})
        owner = options[:owned_by]

        tag_ids = tags.map(&:id)

        tagging_table = arel_taggings_table
        tag_table     = arel_tags_table

        tag_key = tagging_table[:tag_id].eq(tag_table[:id])

        excluded_taggable_ids = tagging_table
            .project(tagging_table[:taggable_id])
            .where(tagging_table[:taggable_type].eq(base_class.name))
            .where(tagging_table[:tag_id].in(tag_ids))

        where(arel_table[primary_key].not_in(excluded_taggable_ids))
      end

      def having_all_tags(tags, options={})
        context = options[:on]
        owner   = options[:owned_by]

        tagging_table     = arel_taggings_table(context: context, for: "group_count")
        taggable_id       = tagging_table[:taggable_id]
        taggable_type     = tagging_table[:taggable_type]

        tag_table         = arel_tags_table(context: context, for: "group_count")

        group_key         = taggable_id.eq(arel_table[primary_key])
                       .and taggable_type.eq(base_class.name)

        same_size         = taggable_id.count.eq(tags.size)

        matching_all_tags = arel_table.join(tagging_table, Arel::Nodes::OuterJoin)
                              .on(group_key).join_sources

        joins(matching_all_tags).group(arel_table[primary_key]).having(same_size)
      end

      unless respond_to?(:none) # Added in Rails 4
        def self.none
          where("1 = 0")
        end
      end

      ##
      # Return a scope of objects that are tagged with the specified tags.
      #
      # @param tags The tags that we want to query for
      # @param [Hash] options A hash of options to alter you query:
      #                       * <tt>:exclude</tt> - if set to true, return objects that are *NOT* tagged with the specified tags
      #                       * <tt>:any</tt> - if set to true, return objects that are tagged with *ANY* of the specified tags
      #                       * <tt>:match_all</tt> - if set to true, return objects that are *ONLY* tagged with the specified tags
      #                       * <tt>:owned_by</tt> - return objects that are *ONLY* owned by the owner
      #
      # Example:
      #   User.tagged_with("awesome", "cool")                     # Users that are tagged with awesome and cool
      #   User.tagged_with("awesome", "cool", :exclude => true)   # Users that are not tagged with awesome or cool
      #   User.tagged_with("awesome", "cool", :any => true)       # Users that are tagged with awesome or cool
      #   User.tagged_with("awesome", "cool", :match_all => true) # Users that are tagged with just awesome and cool
      #   User.tagged_with("awesome", "cool", :owned_by => foo ) # Users that are tagged with just awesome and cool by 'foo'
      def tagged_with(tags, options = {})
        tag_list = ActsAsTaggableOn::TagList.from(tags)
        return none if tag_list.empty?

        tags = ActsAsTaggableOn::Tag.public_send(options[:wild] ? :named_like_any : :named_any, tag_list)
        return none unless (tags.length == tag_list.length) || options[:any] || options[:wild]

        scope = select(arel_table[Arel.star]).joins_taggings(tags, options)
        scope = scope.without_taggings(tags, options) if options[:exclude]
        scope = scope.having_all_tags(tags, options) if options[:match_all]

        scope.uniq(arel_table[:id]).order(options[:order]).readonly(false)
      end

      def is_taggable?
        true
      end

      def taggable_mixin
        @taggable_mixin ||= Module.new
      end
    end

    module InstanceMethods
      def arel_taggings_table(*args)
        self.class.arel_taggings_table(*args)
      end

      def arel_tags_table(*args)
        self.class.arel_tags_table(*args)
      end

      # all column names are necessary for PostgreSQL group clause
      def grouped_column_names_for(object)
        self.class.grouped_column_names_for(object)
      end

      def custom_contexts
        @custom_contexts ||= []
      end

      def is_taggable?
        self.class.is_taggable?
      end

      def add_custom_context(value)
        custom_contexts << value.to_s unless custom_contexts.include?(value.to_s) or self.class.tag_types.map(&:to_s).include?(value.to_s)
      end

      def cached_tag_list_on(context)
        self["cached_#{context.to_s.singularize}_list"]
      end

      def tag_list_cache_set_on(context)
        variable_name = "@#{context.to_s.singularize}_list"
        instance_variable_defined?(variable_name) && !instance_variable_get(variable_name).nil?
      end

      def tag_list_cache_on(context)
        variable_name = "@#{context.to_s.singularize}_list"
        if instance_variable_get(variable_name)
          instance_variable_get(variable_name)
        elsif cached_tag_list_on(context) && self.class.caching_tag_list_on?(context)
          instance_variable_set(variable_name, ActsAsTaggableOn::TagList.from(cached_tag_list_on(context)))
        else
          instance_variable_set(variable_name, ActsAsTaggableOn::TagList.new(tags_on(context).map(&:name)))
        end
      end

      def tag_list_on(context)
        add_custom_context(context)
        tag_list_cache_on(context)
      end

      def all_tags_list_on(context)
        variable_name = "@all_#{context.to_s.singularize}_list"
        return instance_variable_get(variable_name) if instance_variable_defined?(variable_name) && instance_variable_get(variable_name)

        instance_variable_set(variable_name, ActsAsTaggableOn::TagList.new(all_tags_on(context).map(&:name)).freeze)
      end

      ##
      # Returns all tags of a given context
      # @WIP
      def all_tags_on(context)
        tag_table     = base_tags.arel_table
        tagging_table = taggings.arel_table
        for_context   = tagging_table[:context].eq(context.to_s)

        scope = base_tags.where(for_context)

        if ActsAsTaggableOn::Tag.using_postgresql? # FIXME
          group_columns = grouped_column_names_for(ActsAsTaggableOn::Tag)
          scope = scope.order(tagging_table[:created_at].max).group(group_columns)
        else
          scope = scope.group(tag_table[:id])
        end
        scope.to_a
      end

      ##
      # Returns all tags that are not owned of a given context
      def tags_on(context)
        tagging_table = ActsAsTaggableOn::Tagging.arel_table
        for_context   = tagging_table[:context].eq(context.to_s)
        no_tagger     = tagging_table[:tagger_id].eq(nil)

        scope = base_tags.where(for_context.and(no_tagger))
        scope = scope.order(tagging_table[:id]) if self.class.preserve_tag_order?
        scope
      end

      def set_tag_list_on(context, new_list)
        add_custom_context(context)

        variable_name = "@#{context.to_s.singularize}_list"
        process_dirty_object(context, new_list) unless custom_contexts.include?(context.to_s)

        instance_variable_set(variable_name, ActsAsTaggableOn::TagList.from(new_list))
      end

      def tagging_contexts
        custom_contexts + self.class.tag_types.map(&:to_s)
      end

      def process_dirty_object(context,new_list)
        value = new_list.is_a?(Array) ? new_list.join(', ') : new_list
        attrib = "#{context.to_s.singularize}_list"

        if changed_attributes.include?(attrib)
          # The attribute already has an unsaved change.
          old = changed_attributes[attrib]
          changed_attributes.delete(attrib) if (old.to_s == value.to_s)
        else
          old = tag_list_on(context).to_s
          changed_attributes[attrib] = old if (old.to_s != value.to_s)
        end
      end

      def reload(*args)
        self.class.tag_types.each do |context|
          instance_variable_set("@#{context.to_s.singularize}_list", nil)
          instance_variable_set("@all_#{context.to_s.singularize}_list", nil)
        end

        super(*args)
      end

      def save_tags
        tagging_contexts.each do |context|
          next unless tag_list_cache_set_on(context)
          # List of currently assigned tag names
          tag_list = tag_list_cache_on(context).uniq

          # Find existing tags or create non-existing tags:
          tags = ActsAsTaggableOn::Tag.find_or_create_all_with_like_by_name(tag_list)

          # Tag objects for currently assigned tags
          current_tags = tags_on(context)

          # Tag maintenance based on whether preserving the created order of tags
          if self.class.preserve_tag_order?
            old_tags, new_tags = current_tags - tags, tags - current_tags

            shared_tags = current_tags & tags

            if shared_tags.any? && tags[0...shared_tags.size] != shared_tags
              index = shared_tags.each_with_index { |_, i| break i unless shared_tags[i] == tags[i] }

              # Update arrays of tag objects
              old_tags |= current_tags[index...current_tags.size]
              new_tags |= current_tags[index...current_tags.size] & shared_tags

              # Order the array of tag objects to match the tag list
              new_tags = tags.map do |t| 
                new_tags.find { |n| n.name.downcase == t.name.downcase }
              end.compact
            end
          else
            # Delete discarded tags and create new tags
            old_tags = current_tags - tags
            new_tags = tags - current_tags
          end

          # Find taggings to remove:
          if old_tags.present?
            old_taggings = taggings.where(:tagger_type => nil, :tagger_id => nil, :context => context.to_s, :tag_id => old_tags)
          end

          # Destroy old taggings:
          if old_taggings.present?
            ActsAsTaggableOn::Tagging.destroy_all "#{ActsAsTaggableOn::Tagging.primary_key}".to_sym => old_taggings.map(&:id)
          end

          # Create new taggings:
          new_tags.each do |tag|
            taggings.create!(:tag_id => tag.id, :context => context.to_s, :taggable => self)
          end
        end

        true
      end
    end
  end
end
