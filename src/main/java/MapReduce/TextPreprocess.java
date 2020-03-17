package MapReduce;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import static MapReduce.FileHandler.extractGZip;

public class TextPreprocess {
    public static Set<String> stopWordList = new HashSet<String>();

    /**
     * Read from stopword file.
     * 
     * @param stopWordFile_PATH
     */
    public void readStopWordFile(String stopWordFile_PATH) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(stopWordFile_PATH));
        } catch (IOException ioe) {
            System.err.println("Exception while reading stop word file" + stopWordFile_PATH + " " + ioe.toString());
        }
    }

    /**
     * Extract .tar file.
     * 
     * @param filePath
     */
    public static void textPreprocess(String filePath) {
        try {
            File tarFile = new File(filePath);
            String par_dir = tarFile.getParent();
            System.out.println(tarFile.getParent());
            // File outFile = new File()
            // extractGZip(tarFile, )
        } catch (Exception ioe) {
            System.err.println("Exception while reading tar file" + filePath + " " + ioe.toString());
        }
    }

    /**
     * Remove all subtitle in the input text file.
     * 
     * Since we need to split the file into a series of articles, for convinience,
     * it would be easier without subtitles.
     * 
     * The formate of subtitle can be represented in regular expression `={2}.*={2}`
     * 
     * @todo 1. implement stopword removal; 2. Remove languages charactors other
     *       than English.
     * 
     * 
     * @author Molin Liu
     * @param filePath
     * @return File
     * @throws IOException
     */
    public static File textCleaner(String filePath) throws IOException {

        File file = new File(filePath);
        File temp = File.createTempFile("file", ".txt", file.getParentFile());
        String charset = "UTF-8";
        String delete = "medical_data";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(temp), charset));
        String currentLine;
        while ((currentLine = reader.readLine()) != null) {
            currentLine = currentLine.replaceAll("={2}.*={2}", "");
            writer.println(currentLine);
        }
        writer.close();
        reader.close();
        return temp;
    }

    public static void main(String[] args) throws Exception {
        textCleaner("/home/molin/Documents/Data/input/sample.txt");
        // String testTarFile =
        // "/home/molin/Documents/Data/input/20140615-wiki-en_000000.txt.gz";
        // textPreprocess(testTarFile);
    }

}
