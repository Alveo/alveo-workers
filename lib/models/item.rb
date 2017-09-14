class Item < ActiveRecord::Base

  belongs_to :collection
  has_many :documents, dependent: :destroy
  
  validates :uri, presence: true
  # validates :handle, presence: true, uniqueness: {case_sensitive: false}
  validates :handle, presence: true, uniqueness: true

end
