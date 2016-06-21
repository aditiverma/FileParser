import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/*
 * create connection with database and run queries
 * */

class DatabaseConnect {
	final static String URL = "jdbc:mysql://localhost/test";
	final static String username = "root";
	final static String pass = "root";
	static Connection conn = null;

	DatabaseConnect() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(URL, username, pass);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	void closeConnection() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * table metadata stores the names of data and specs files that have been read from file system and 
	 * stored in database. This is useful to avoid re-processing the same data/spec file
	 */
	
	void createMetaTable() {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "create table if not exists metadata (file_name varchar(255))";
			stmt.executeUpdate(sql);
			System.out.println("meta table created");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * when the application is started, this function loads the specs and data files and stores them
	 * in database
	 * */
	void populateAllTables() {
		String currentDir = System.getProperty("user.dir");
		File specsDirectory = new File(currentDir + "/specs/");
		File dataDirectory = new File(currentDir + "/data/");
		File[] listOfSpecsFiles = specsDirectory.listFiles();
		for (File file : listOfSpecsFiles) {
			if (file.isFile()) {
				//create table with schema from spec file
				createTable(file);
				String specFile = getFileName(file);
				File[] allDataFiles = dataDirectory.listFiles();
				for (int i = 0; i < allDataFiles.length; i++) {
					String dataFileName = allDataFiles[i].getName();
					if (dataFileName.startsWith(specFile)) {
						//populate content of data files into the table 
						populateData(allDataFiles[i]);
					}
				}
			}
		}
	}

	/*
	 * get file name without type extension (removes the extensions like .csv, .txt )
	 */
	String getFileName(File file) {
		String fileName = file.getName();
		return fileName.split("\\.")[0];
	}

	/*
	 * create table from schema specified in specs file
	 * */
	void createTable(File file) {
		try {
			// check if file already exists in metadata (table was created before)
			if (metaDataContains(file)) {
				return;
			}
			ReadFiles reader = new ReadFiles();
			//read contents of specs file and store as a list of strings
			List<String> specs = reader.getSpecContents(file);
			StringBuilder arguments = new StringBuilder();
			Statement stmt = null;
			//for each line in the file in /specs, split the line by ',' and store as arguments
			for (int i = 0; i < specs.size(); i++) {
				String[] cols = specs.get(i).split(",");
				arguments.append(cols[0]+ " "+ cols[2]+ ""+ (cols[2].toLowerCase().equals("boolean") ? "" : "("+ cols[1] + ")"));
				if (i < specs.size() - 1)//
				{
					arguments.append(",");
				}
			}
			stmt = conn.createStatement();
			String sql = "create table " + getFileName(file) + " (" + arguments+ ");";
			stmt.executeUpdate(sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (NullPointerException e)
		{
			e.printStackTrace();
		}
		insertFileNameIntoMetaTable(file);
	}

	/*
	 * populate data file into the corresponding table
	 * */
	void populateData(File dataFile) {
		// check if dataFile exists in metadata table (to check if file contents were already populated into table)
		if (metaDataContains(dataFile)) {
			return;
		}
		String fileName = getFileName(dataFile);
		String specFileName = fileName.substring(0, fileName.indexOf('_'));
		try {
			Statement stmt;
			stmt = conn.createStatement();
			String sql = "load data local infile '"+ dataFile.getAbsolutePath() + "' into table "+ specFileName;// check all formats
			stmt.executeUpdate(sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		insertFileNameIntoMetaTable(dataFile);
	}

	/*
	 * To avoid reading a file twice and avoid duplicate writes to table, add every file to metadata table after first read 
	 * */
	void insertFileNameIntoMetaTable(File file) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "insert into metadata values (\'" + getFileName(file)+ "\')";
			stmt.executeUpdate(sql);
			System.out.println("file added to meta table " + file.getName());
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Check if metadata contains file name (if file has been read once and inserted into table)
	 * */
	boolean metaDataContains(File file) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "select file_name from  metadata where file_name=\'"+ getFileName(file) + "\'";
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				return true;
			}
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
}
