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
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(URL, username, pass);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	void closeConnection()
	{
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*
	 * table metadata stores the names of data files that have been stored in
	 * database. This is useful to avoid re-processing the same data file
	 */
	void createMetaTable() {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "create table if not exists metadata (file_name varchar(255))";
			int rs = stmt.executeUpdate(sql);
			System.out.println("meta table created");
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	void populateAllTables() {
		String currentDir = System.getProperty("user.dir");
		File specsDirectory = new File(currentDir + "/specs/");
		File dataDirectory = new File(currentDir + "/data/");
		File[] listOfSpecsFiles = specsDirectory.listFiles();

		for (File file : listOfSpecsFiles) {
			if (file.isFile()) {

				System.out.println("spec file " + getFileName(file));
				createTable(file);
	
				//String specFile = file.getName();
				//String specFormat = specFile.split("\\.")[0];
				
				String specFile = getFileName(file);
				
				File[] allDataFiles = dataDirectory.listFiles();
				for (int i = 0; i < allDataFiles.length; i++) {

					String dataFileName = allDataFiles[i].getName();
					if (dataFileName.startsWith(specFile)) {// check file name
																// correct
						populateData(allDataFiles[i]);

					}
				}

			}
		}
	}

	/*
	 * get file name without type extension (type can be .csv or .txt )
	 * */
	 String getFileName(File file)
	{
		String fileName=file.getName();
		return fileName.split("\\.")[0];
		
	}
	 void createTable(File file) {
		// check if file exists in metadata table
		try {
			if (metaDataContains(file)) {
				System.out.println("spec file added to meta");
				return;
			}
			updateMetaTable(file);
			ReadFiles reader = new ReadFiles();
			List<String> specs = reader.getSpecContents(file);// check if
																// size==0
			StringBuilder arguments = new StringBuilder();
			Statement stmt = null;
			for (int i = 0; i < specs.size(); i++) {
				String[] cols = specs.get(i).split(",");// check if array size=3
				arguments.append(cols[0]
						+ " "
						+ cols[2]
						+ ""
						+ (cols[2].toLowerCase().equals("boolean") ? "" : "("
								+ cols[1] + ")"));
				if (i < specs.size() - 1)//
				{
					arguments.append(",");
				}
			}
			stmt = conn.createStatement();
			String sql = "create table " + getFileName(file) + " (" + arguments
					+ ");";
			int rs = stmt.executeUpdate(sql);
			System.out.println("spec table created "+file.getName());
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	 void populateData(File dataFile) {
		// check if dataFile exists in metadata table
		if (metaDataContains(dataFile)) {
			
			System.out.println("data file added");
			return;
		}
		
		updateMetaTable(dataFile);
		String fileName = getFileName(dataFile);
		String specFileName = fileName.substring(0, fileName.indexOf('_'));
		// String dt=fileName.substring(fileName.indexOf("_")+1,
		// fileName.indexOf("."));
		try {
			Statement stmt;
			stmt = conn.createStatement();
			String sql = "load data local infile '"
					+ dataFile.getAbsolutePath() + "' into table "
					+ specFileName;// check all formats

			int rs = stmt.executeUpdate(sql);
			System.out.println("file content loaded in data table "+fileName);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	 void updateMetaTable(File file) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "insert into metadata values (\'" + getFileName(file)
					+ "\')";
			int rs = stmt.executeUpdate(sql);
			System.out.println("file added to meta table "+file.getName());
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	 boolean metaDataContains(File file) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "select file_name from  metadata where file_name=\'"
					+ getFileName(file) + "\'";
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
