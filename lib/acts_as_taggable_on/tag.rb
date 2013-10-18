module ActsAsTaggableOn
  class Tag < ::ActiveRecord::Base
    include ActsAsTaggableOn::Utils

    attr_accessible :name if defined?(ActiveModel::MassAssignmentSecurity)

    ### ASSOCIATIONS:

    has_many :taggings, :dependent => :destroy, :class_name => 'ActsAsTaggableOn::Tagging'

    ### VALIDATIONS:

    validates_presence_of :name
    validates_uniqueness_of :name, :if => :validates_name_uniqueness?
    validates_length_of :name, :maximum => 255

    # monkey patch this method if don't need name uniqueness validation
    def validates_name_uniqueness?
      true
    end

    ### SCOPES:

    def self.named(name)
      if ActsAsTaggableOn.strict_case_match
        where(["name = #{binary}?", name])
      else
        where(["lower(name) = ?", name.downcase])
      end
    end

    def self.named_any(list)
      if ActsAsTaggableOn.strict_case_match
        where(list.map { |tag| sanitize_sql(["name = #{binary}?", tag.to_s.mb_chars]) }.join(" OR "))
      else
        where(list.map { |tag| sanitize_sql(["lower(name) = ?", tag.to_s.mb_chars.downcase]) }.join(" OR "))
      end
    end

    def self.named_like(name)
      where(escaped_name_attribute_matches(name))
    end

    def self.named_like_any(list)
      where(Array(list).map { |name| escaped_name_attribute_matches(name) }.reduce(&:or))
    end

    def self.escaped_name_attribute_matches(name)
      arel_table[:name].matches(Arel.sql(%{'%#{name.gsub('%','!%')}%' ESCAPE '!'}))
    end

    ### CLASS METHODS:

    def self.find_or_create_with_like_by_name(name)
      if ActsAsTaggableOn.strict_case_match
        find_or_create_all_with_like_by_name([name]).first
      else
        named_like(name).first || create(:name => name)
      end
    end

    def self.find_or_create_all_with_like_by_name(*list)
      list = Array(list).flatten
      return list if list.empty?

      existing_tags = named_any(list)

      list.map do |tag_name|
        existing_tag = existing_tags.find { |tag| comparable_name(tag.name) == comparable_name(tag_name) }
        existing_tag or create(name: tag_name)
      end
    end

    ### INSTANCE METHODS:

    def ==(object)
      super or object.is_a?(Tag) && name == object.name
    end

    def to_s
      name
    end

    def count
      read_attribute(:count).to_i
    end

    class << self
      private

      def comparable_name(str)
        str.mb_chars.downcase.to_s
      end

      def binary
        /mysql/ === ActiveRecord::Base.connection_config[:adapter] ? "BINARY " : nil
      end
    end
  end
end
