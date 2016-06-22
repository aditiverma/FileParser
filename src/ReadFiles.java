import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class ReadFiles {
  public List<String> getSpecContents(File file) {
		List<String> specLines = new ArrayList<String>();
		BufferedReader in;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));
			String str;
			in.readLine();
			while ((str = in.readLine()) != null) {
					specLines.add(str);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return specLines;
	}
}
