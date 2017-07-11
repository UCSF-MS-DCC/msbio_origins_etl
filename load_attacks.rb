require 'csv'
require 'httparty'

csv_text = File.read('/Users/adamrenschen/Desktop/msbio_origins_etl/msb_origins_attacks_reformatted.csv')
csv = CSV.parse(csv_text, :headers => true)
@flag = false
csv.each do |row|
  @res = HTTParty.post("https://uat.champagne.ucsf.edu/api/v1/attacks", :body => row.to_hash.to_json, :headers => { 'Content-type':'application/json', 'Authorization':'Token token="AUTH TOKEN GOES HERE"' })
  if !@flag
    puts @res
    @flag = true
  end
end
