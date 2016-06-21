import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.WatchEvent.Kind;

/*
 * This clqss will watch the directories /spec and /data for new files, and add the new
 * files into database
 * */
class WatcherService {

	static void loadExistingFiles() {
		// create connection with database
		DatabaseConnect dc = new DatabaseConnect();

		// create metadata once to store data files and spec files loaded in database
		dc.createMetaTable();

		// load existing /spec and /data files into database
		dc.populateAllTables();
		
		dc.closeConnection();

	}

	static void loadNewFiles() {

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
		
		//watchDirectoryEvents(data);
		//watchDirectoryEvents(specs);

	}

	static  void watchDirectoryEvents(String dir) {

		try {
			String currentDir = System.getProperty("user.dir");
			String path = currentDir + dir;

			// Sanity check - Check if path is a folder
			File folder = new File(currentDir);
			Path currPath = Paths.get(path);
			// Boolean isFolder = currPath.isDirectory()
			// Boolean isFolder = (Boolean)
			// Files.getAttribute(currPath,"basic:isDirectory", NOFOLLOW_LINKS);
			if (!folder.isDirectory()) {
				throw new IllegalArgumentException("Path: " + folder
						+ " is not a folder");
			}
			System.out.println("Watching path: " + currPath);

			// We obtain the file system of the Path
			FileSystem fs = currPath.getFileSystem();

			WatchService service = fs.newWatchService();
			// register the path to the service and watch for creation events
			currPath.register(service, ENTRY_CREATE);
			int count=1;
			// Start the infinite polling loop
			while (true) {
				WatchKey key = service.take();

				// Dequeueing events
				Kind kind = null;
				
				for (WatchEvent watchEvent : key.pollEvents()) {
					// Get the type of the event
					kind = watchEvent.kind();
					if (ENTRY_CREATE == kind) {
						// A new Path was created
						// Path newPath = ((WatchEvent<Path>)
						// watchEvent).context();
						Path newPath = (Path) watchEvent.context();
						// Output
						System.out.println("New path created: " + newPath);
						DatabaseConnect dc = new DatabaseConnect();
						
						
						if(dir.equals("/data/"))
						{
							File dataFile = new File(currentDir + "/data/"+newPath.toString());
							dc.populateData(dataFile);
						}
						else
						{
							File specsFile = new File(currentDir + "/specs/"+newPath.toString());
							dc.createTable(specsFile);
							System.out.println("!!!!!!!!!!"+count++);
						}
						dc.closeConnection();
						
					}
					
					
				}

				if (!key.reset()) {
					break; // loop
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
