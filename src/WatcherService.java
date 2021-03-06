
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.WatchEvent.Kind;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/*
 * This clqss will watch the directories /spec and /data for new files, and add the newly dropped
 * files into database
 * */
class WatcherService {
  
  private static Queue<File> dataFilesWithoutASpecFile = new LinkedList<File>();
	public static void loadExistingFiles() {
		// create connection with database
		DatabaseConnect dc = new DatabaseConnect();
		// create metadata once to store data files and spec files loaded in database
		dc.createMetaTable();
		// load existing /spec and /data files into database
		dc.populateAllTables();		
		dc.closeConnection();

	}

	public static void loadNewFiles() {
		final String data = "/data/";
		final String specs = "/specs/";
		/*
		 * watch /data and /spec directories in parallel using threads
		 * */
		Thread thread1 = new Thread() {
		    public void run() {
		    	watchDirectoryEvents(data);
		    }
		};
		Thread thread2 = new Thread() {
		    public void run() {
		    	watchDirectoryEvents(specs);
		    }
		};
		thread1.start();
		thread2.start();
	}

	public static  void watchDirectoryEvents(String dir) {
		try {
			String currentDir = System.getProperty("user.dir");
			String path = currentDir + dir;
			//Check if path is a folder
			File folder = new File(currentDir);
			Path currPath = Paths.get(path);
			if (!folder.isDirectory()) {
				throw new IllegalArgumentException("Path: " + folder+ " is not a folder");
			}
			System.out.println("Watching path: " + currPath);
			//obtain the file system of the Path
			FileSystem fs = currPath.getFileSystem();
			WatchService service = fs.newWatchService();
			// register the path to the service and watch for creation events
			currPath.register(service, ENTRY_CREATE);
			// Start the infinite polling loop
			while (true) {
				WatchKey key = service.take();
				// Dequeueing events
				Kind kind = null;			
				for (WatchEvent watchEvent : key.pollEvents()) {
					// Get the type of the event
					kind = watchEvent.kind();
					if (ENTRY_CREATE == kind) {
						//If a new path is created, insert the contents of file into database
						Path newPath = (Path) watchEvent.context();
						DatabaseConnect dc = new DatabaseConnect();
						File newFile = new File(currentDir + dir+newPath.toString());
						if(dir.equals("/data/")){
						  //check if spec (schema definition) for this data file exists
						  if(dc.doesSchemaExistForDataFile(newPath.toFile())){
	              //insert contents of new data file into table
	              dc.populateData(newFile);
						  }
						  else{
						    dataFilesWithoutASpecFile.add(newPath.toFile());						    
						  }

						}
						else{
							//create table with the contents of new specs file
							dc.createTable(newFile);
							//check if data files for this new schema exist in the queue
							for(File dataFile:dataFilesWithoutASpecFile){
							  if(newPath.toFile().toString().split("\\.")[0].equals(dataFile.toString().split("_")[0])){
							    dc.populateData(newFile);
							    dataFilesWithoutASpecFile.remove(dataFile);
							  }
							    
							}
						}
					}
				}

				if (!key.reset()) {
					break; 
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException ioe) {
			// Folder does not exists
			ioe.printStackTrace();
		}
	}
}
