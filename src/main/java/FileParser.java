import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.logging.Level;

public class FileParser {
    public void open(String path) throws FileNotFoundException {
        File file = new File(path);
        scanner = new Scanner(file);
    }

    public void close() {
        scanner.close();
    }

    public Map<String, String> parseTxt() throws RuntimeException {
        Map<String, String> result = new HashMap<String, String>();
        while (scanner.hasNextLine()) {
            String[] entry = scanner.nextLine().split(":");
            if (entry.length != 2) {
                Main.LOG.log(Level.SEVERE, "Invalid txt file format");
                throw new RuntimeException("[Error][FileParser] Invalid file format!");
            }
            String key = entry[0].trim();
            String value = entry[1].trim();
            result.put(key, value);
        }
        scanner.close();
        return result;
    }

    public List<Map<String, String>> parseCsv() {
        List<Map<String, String>> result = new ArrayList<Map<String, String>>();
        if (!scanner.hasNextLine()) {
            throw new RuntimeException("[Error][FileParser][CSV] Csv must have a header");
        }
        String[] header = scanner.nextLine().split(",");
        while (scanner.hasNextLine()) {
            String[] rawEntry = scanner.nextLine().split(",");
            if (rawEntry.length != header.length) {
                Main.LOG.log(Level.SEVERE, "Invalid csv file format");
                throw new RuntimeException("[Error][FileParser] Invalid file format!");
            }
            Map<String, String> entry = new HashMap<String, String>();
            for (int i = 0; i < rawEntry.length; ++i) {
                entry.put(header[i], rawEntry[i]);
            }
            result.add(entry);
        }
        return result;
    }

    private Scanner scanner;
}
