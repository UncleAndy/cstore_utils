import java.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;

import java.util.ArrayList;

/*
	1. Получаем foreign_server из information_schema.foreign_servers (для объявления CREATE SERVER)
	с foreign_data_wrapper_name == 'cstore_fdw';
	2. Перебор таблиц из information_schema.foreign_tables у которых foreign_server_name - один из выбранных;
	3. Получаем OPTIONS из information_schema.foreign_table_options;
	4. Получаем информацию о том, не является-ли таблица inherit из pg_inherits и, если является, получаем
	главную таблицу из pg_class и констрэйн check из pg_constraint;
	5. Получаем данные о столбцах из information_schema.columns;
	6. Формируем DDL для талиц.
	7. Извлекаем данные из таблиц и формируем COPY блок с данными для каждой таблицы.
 */

class CstoreTable {
    String schema;
    String name;
    String foreign_server;
    String foreign_options;
    String inherit_main_table;
    String inherit_check_constrains;
    ArrayList<String> columns;
    String ddl;
}

class CstoreDump
{
    public static void main(String args[]){
        ArrayList<CstoreTable> cstore_tables = new ArrayList<CstoreTable>();

        System.out.println("Start backup cstore tables...");

        Connection c = null;
        Statement stmt = null;
        try {
           Class.forName("org.postgresql.Driver");
           c = DriverManager
              .getConnection("jdbc:postgresql://localhost:5433/test",
              "postgres", "");
           c.setAutoCommit(false);
           System.out.println("Opened database successfully");

           stmt = c.createStatement();
           ResultSet rs = stmt.executeQuery( 
        		   	"SELECT * "+
	   				"FROM "+
	   					"information_schema.foreign_tables ft, "+
				   		"information_schema.foreign_servers fs "+
			   		"WHERE "+
				   		"fs.foreign_data_wrapper_name = 'cstore_fdw' AND "+
				   		"fs.foreign_server_name = ft.foreign_server_name;" 
        		   );
           while ( rs.next() ) {
              CstoreTable cs_table 	= new CstoreTable();
              cs_table.schema 		= rs.getString("foreign_table_schema");
              cs_table.name 		= rs.getString("foreign_table_name");
              cs_table.foreign_server = rs.getString("foreign_server_name");
              cstore_tables.add(cs_table);
           }
           rs.close();
           
           System.out.println( "Founded tables count: " + cstore_tables.size() );
           
       	   for ( CstoreTable t : cstore_tables ) {
       		   
       		   // cstore options
       		   rs = stmt.executeQuery(
        		   "SELECT option_name, option_value " +
        		   "FROM information_schema.foreign_table_options " +
        		   "WHERE " +
        		   "foreign_table_schema = '"+t.schema+"' AND " +
    		  	   "foreign_table_name = '"+t.name+"';"
    		   );
       		   if ( t.foreign_options == null ) { t.foreign_options = ""; };
               while ( rs.next() ) {
            	   t.foreign_options = t.foreign_options + rs.getString("option_name") + " '" + rs.getString("option_value") + "'";
            	   System.out.println( "Options for table: " + t.name + ": " + t.foreign_options );
               };
               rs.close();
        	   
               // Find inherit main table if exists
               rs = stmt.executeQuery(
        		   "SELECT p.relname as parent_table, n.nspname as parent_schema " +
               		"FROM " +
               			"pg_inherits i, " +
               			"pg_class p " +
               			"JOIN pg_catalog.pg_namespace n ON n.oid = p.relnamespace " +
           			"WHERE i.inhrelid = '" + t.schema + "." + t.name + "'::regclass::oid and p.oid = i.inhparent"
   			   );
               while ( rs.next() ) {
            	   t.inherit_main_table = rs.getString("parent_schema") + "." + rs.getString("parent_table");
            	   System.out.println( "Parent for table: " + t.name + " = " + t.inherit_main_table );
               };
               rs.close();
        	   
        	   // Find check constrains
               rs = stmt.executeQuery(
        		   "SELECT consrc " +
               		"FROM " +
               			"pg_constraint " +
           			"WHERE contype = 'c' AND conrelid = '" + t.schema + "." + t.name + "'::regclass::oid"
   			   );
               while ( rs.next() ) {
            	   t.inherit_check_constrains = rs.getString("consrc");
            	   System.out.println( "Check constrains for table: " + t.name + " = " + t.inherit_check_constrains );
               };
               rs.close();
        	   
               // Columns
               t.columns = new ArrayList<String>();
               rs = stmt.executeQuery(
            		   "SELECT c.column_name, c.column_default, c.is_nullable, pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type " +
            		   "FROM information_schema.columns c, pg_catalog.pg_attribute a " +
            		   "WHERE " +
            		   "c.table_schema = '" + t.schema + "' AND c.table_name = '" + t.name + "' AND " +
        		   	   "a.attname = c.column_name AND a.attrelid = '" + t.schema + "." + t.name + "'::regclass::oid;"
   			   );
               while ( rs.next() ) {
            	   String column_definition;
            	   String column_name;
            	   column_name = rs.getString("column_name");
            	   column_definition = column_name;
            	   
        		   column_definition += " " + rs.getString("data_type");
            	   
            	   String nullable = rs.getString("is_nullable");
            	   if ( nullable == "NO" ) {
            		   column_definition += " NOT NULL ";
            	   };
            	   
            	   String default_val = rs.getString("column_default");
            	   if ( !rs.wasNull() ) {
            		   column_definition += " DEFAULT " + default_val;
            	   };
            	   
            	   t.columns.add(column_definition);
            	   System.out.println( "Column for table: " + t.name + " = " + column_definition );
               };
               rs.close();
               
        	   // create table ddl
        	   t.ddl = "CREATE FOREIGN TABLE " + t.schema + "." + t.name + " (\n";
        	   String sep = "\t";
               for ( String column : t.columns ) {
            	   t.ddl += sep + column;
            	   sep = ",\n\t";
               }

               // check constraint
               if ( t.inherit_check_constrains != null && !t.inherit_check_constrains.isEmpty() ) {
            	   t.ddl += sep + "CHECK " + t.inherit_check_constrains; 
               }
               
               t.ddl += "\n)\n";

               // inherit table
               if ( t.inherit_main_table != null && !t.inherit_main_table.isEmpty() ) {
            	   t.ddl += "INHERITS (" + t.inherit_main_table + ")\n";
               }
               
               // foreign params
               t.ddl += "SERVER " + t.foreign_server + "\n";
               t.ddl += "OPTIONS ( " + t.foreign_options + " )";
        	   
               
               t.ddl += ";";
               
        	   System.out.println( "DDL for table: " + t.ddl );
           }
           
           
           
           
           
           
           
           stmt.close();
           c.close();
        } catch ( Exception e ) {
           System.err.println( e.getClass().getName()+": "+ e.getMessage() );
           System.exit(0);
        }




    }

    
}
