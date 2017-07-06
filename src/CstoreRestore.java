import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/*
 * Порядок работы:
 * 1. Файл бэкапа разворачивается во временный катало с именем = имени БД;
 * 2. Выбираются все ddl.sql файлы и исполняются в БД;
 * 3. Выбираются все .gz файлы и в таблицу из имени файла делается COPY с разжатием (gunzip) 
 */

class CstoreRestore
{
    public static void main(String args[]) {
        System.out.println("Start...");

    	Option p_host = new Option("h", "host", true, "postgres db host");
        Option p_port = new Option("p", "port", true, "postgres db port");
        Option p_db_name = new Option("d", "dbname", true, "postgres db name");
        Option p_db_user = new Option("u", "dbuser", true, "postgres db user");
        Option p_db_pass = new Option("w", "dbpass", true, "postgres db password");
        Option p_tmp_path = new Option("t", "tmp_path", true, "path for temporary files");
        
        Options options = new Options();
        options.addOption(p_host);
        options.addOption(p_port);
        options.addOption(p_db_name);
        options.addOption(p_db_user);
        options.addOption(p_db_pass);
        options.addOption(p_tmp_path);

        String host = "";
        String port = "";
        String db_name = "";
        String db_user = "";
        String db_pass = "";
        
        String backup_path = "";
        String temp_path = "";
        
        CommandLineParser parser = new DefaultParser();
        File theDir;
        try {
			CommandLine line = parser.parse( options, args );
			
			if ( !line.hasOption("h") || 
					!line.hasOption("d") || 
					!line.hasOption("u")) {
				throw new ParseException("Absent required option.");
			};
			
			host = line.getOptionValue("h", "localhost");
			port = line.getOptionValue("p", "5432");
			db_name = line.getOptionValue("d", "postgres");
			db_user = line.getOptionValue("u", "postgres");
			db_pass = line.getOptionValue("w", "");
			temp_path = line.getOptionValue("t", "/tmp");
			
			if ( args.length % 2 != 0 ) {
				backup_path = args[ args.length - 1 ];
			} else {
				throw new ParseException("Backup file name required.");
			}
		} catch (ParseException e1) {
			System.out.println(e1.getMessage());
			HelpFormatter formatter = new HelpFormatter(); 
			formatter.printHelp( "cstore_restore [options] <backup file name>", options );
			System.exit(0);
		}
        
        theDir = new File(temp_path + "/" + db_name);
        if ( !theDir.exists() ) {
            System.out.println("creating directory: " + theDir.getName() + "...");
            boolean result = false;

            try{
                theDir.mkdir();
                result = true;
            } 
            catch(SecurityException se){
                //handle it
            }        
            if(result) {    
                System.out.println("DIR created");  
            }
        };
        theDir.setWritable(true, false);
        
        System.out.println( "Extore from tar: " + backup_path + "...");
        Process tar;
		try {
			tar = Runtime.getRuntime().exec("tar xf " + backup_path + " --directory " + temp_path + "/" + db_name + "/ .");
	        tar.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println( "Extore from tar: " + backup_path + " finished");
        
        
        Connection c = null;
        Statement stmt = null;
    	try {
        	Class.forName("org.postgresql.Driver");
           	c = DriverManager
              .getConnection("jdbc:postgresql://" + host + ":" + port + "/" + db_name,
              db_user, db_pass);
           	c.setAutoCommit(false);
           	System.out.println("Opened database successfully");

           	stmt = c.createStatement();

           	// DDL
            FilenameFilter only_sql = new FilenameFilter() {
    			@Override
    			public boolean accept(File dir, String name) {
    				String ext = name.substring(name.length() - 8, name.length());
    				return (ext.equals(".ddl.sql"));
    			}
            };
	        for (File file: theDir.listFiles(only_sql)) {
	            System.out.println( "Run SQL file name: " + file.getName());
	            
	            FileReader fr = new FileReader(file);
	            char[] chars = new char[(int) file.length()];
	            fr.read(chars);
	            String sql = new String(chars);
	            fr.close();
	            
	            if ( !sql.isEmpty() ) {
	            	stmt.execute(sql);
	            }
	        };
	        
	        // COPY
	        FilenameFilter only_data = new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					String ext = name.substring(name.length() - 3, name.length());
					return (ext.equals(".gz"));
				}
	        };
	        for (File file: theDir.listFiles(only_data)) {
	            System.out.println( "Data file name: " + file.getName());
	            
	            String data_path = file.getAbsolutePath();
	            String filename = file.getName();
	            String table_name = filename.substring(0, filename.length() - 3);
	            
	            System.out.println( "Copy data for: " + table_name + "...");
	            stmt.execute("COPY " + table_name + " FROM PROGRAM 'gunzip " + data_path + " -c';");
	            System.out.println( "Copy data for: " + table_name + " done");
	        };

	        System.out.println("Commit...");
	        c.commit();
	        System.out.println("Commit done");
	        System.out.println( "Clean temporary files...");
	        clean_tmp_files(theDir);
	        System.out.println( "Clean temporary files done");
        } catch ( Exception e ) {
            System.out.println( e.getMessage() );
	        System.out.println("Rollback...");
			try {
				c.rollback();
			} catch (SQLException e1) {
			}
	        System.out.println("Rollback done");
	        System.out.println( "Clean temporary files...");
	        clean_tmp_files(theDir);
	        System.out.println( "Clean temporary files done");
            System.exit(0);
        }
        System.out.println("Finish");
    };
    
    static void clean_tmp_files(File dir) {
        for (File file: dir.listFiles()) 
    	    if (file.isFile()) file.delete();
    };
}
