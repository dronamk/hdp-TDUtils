#export CLASSPATH=$CLASSPATH:/usr/lib/hadoop/lib/terajdbc4.jar:/usr/lib/hadoop/lib/tdgssconfig.jar
#export TABLENAME=hadoop_work_tbls.hdp_fld_tst_tbl
#export INFILE=hdpfldtstfile.txt
#export DELIMITER="|"

jruby << EOF
require 'java'

tablename = ENV['TABLENAME']
username  = ENV['TERADATA_USERNAME']
password  = ENV['TERADATA_PASSWORD']
tera_jdbc = ENV['TERADATA_JDBC']
fl_host   = ENV['TERADATA_HOST']
inp_file  = ENV['INFILE']
delim     = ENV['DELIMITER']

com.teradata.jdbc.TeraDriver
conn = java.sql.DriverManager.get_connection('jdbc:teradata://'"#{tera_jdbc}", "#{username}", "#{password}")

stmt = conn.create_statement
stmt.methods
rs = stmt.execute_query("select top 1 * from #{tablename}  where 1=0")
rsmd = rs.get_meta_data
num_of_columns = rsmd.get_column_count

if num_of_columns == 0
  puts "ERROR, didn't detect any columns"
  exit 1
end

puts ".SESSIONS 2;"
puts ".ERRLIMIT 25;"
puts ".LOGON #{fl_host}/#{username},#{password};"
puts "SET RECORD VARTEXT \"#{delim}\" DISPLAY_ERRORS NOSTOP;"
puts "DELETE FROM #{tablename} ALL;"
puts "ERRLIMIT 2;"


def_str = ""
def_str << "DEFINE\n"
ins_str = ""
ins_str << "INSERT INTO #{tablename}(\n"
bind_str = " VALUES ("

(1..num_of_columns).each do |i|
  column_type = rsmd.get_column_type_name(i)
  column_name = rsmd.get_column_name(i)
  column_scale = rsmd.get_scale(i)
  column_precision = rsmd.get_precision(i)
  column_display_size = rsmd.get_column_display_size(i)

#puts "#{column_type}, #{column_name}, #{column_scale}, #{column_precision}, #{column_display_size}"

  case column_type
  when 'DATE'
    def_str << "#{column_name} (varchar(#{column_display_size + 1}))\n"
  when 'TIME'
    def_str << "#{column_name} (varchar(#{column_display_size}))\n"
  when 'TIMESTAMP'
    def_str << "#{column_name}  (varchar(#{column_display_size}))\n"
  when 'CHAR'
    def_str << "#{column_name} (varchar(#{column_display_size}))\n"
  when 'INTEGER'
    def_str << "#{column_name} (varchar(11))\n"
  when 'BIGINT'
    def_str << "#{column_name} (varchar(#{column_display_size}))\n"
  when 'BYTEINT'
    def_str << "#{column_name} (varchar(#{column_display_size}))\n"
  when 'SMALLINT'
    def_str << "#{column_name} (varchar(6))\n"
  when 'DECIMAL'
	if(column_scale != 0)
  		stringDashDec=""
		for i in 1..(column_scale)
		 stringDashDec=stringDashDec + "9"
  		end
 		def_str << "#{column_name} (varchar(#{column_precision + 2}))\n"
	else
    		def_str << "#{column_name} (varchar(#{column_precision + 1}))\n"
	end
  else
    def_str <<  "#{column_name}  (varchar(#{column_display_size}))\n"
  end
  if i!=num_of_columns
	 def_str << ","
    	 bind_str << ":#{column_name},\n"
    	 ins_str << "#{column_name},\n"
  else
    	 bind_str << ":#{column_name} );\n"
    	 ins_str << "#{column_name})\n"
  end

end
def_str << "FILE=#{inp_file};\n"
puts def_str
puts "show;"
puts "BEGIN LOADING #{tablename} ERRORFILES #{tablename}_E1, #{tablename}_E2;"
ins_str << bind_str
puts ins_str

puts ".END LOADING;"
puts ".LOGOFF;"

EOF

