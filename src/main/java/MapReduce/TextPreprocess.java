package MapReduce;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static MapReduce.FileHandler.extractGZip;

/**
 * Preprocess the text file before feed into MapReduce.
 */
public class TextPreprocess {
    /**
     * The Stop word list.
     */
    public static List<String> stopWordList;
    /**
     * The constant titlePattern.
     */
    public static final Pattern titlePattern = Pattern.compile("^\\[{2}.*\\]{2}");

    /**
     * Instantiates a new Text preprocess.
     */
    public TextPreprocess(){
        readStopWordFile("src/main/resources/stopword-list.txt");
    }

    /**
     * Read from stopword file.
     * 
     * @param stopWordFile_PATH
     */
    public void readStopWordFile(String stopWordFile_PATH) {
        try {
            //BufferedReader fis = new BufferedReader(new FileReader(stopWordFile_PATH));
            this.stopWordList = Files.readAllLines(Paths.get(stopWordFile_PATH));
        } catch (IOException ioe) {
            System.err.println("Exception while reading stop word file" + stopWordFile_PATH + " " + ioe.toString());
        }
    }

    /**
     * Extract .tar file.
     * @param filePath the file path
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
     * <p>
     * Since we need to split the file into a series of articles, for convinience,
     * it would be easier without subtitles.
     * <p>
     * The formate of subtitle can be represented in regular expression `={2}.*={2}`
     *
     * @param filePath the file path
     * @return File file
     * @throws IOException the io exception
     * @author Molin Liu
     */
    public static File textCleaner(String filePath) throws IOException {
        File file = new File(filePath);
        File temp = File.createTempFile("file", ".txt", file.getParentFile());
        String charset = "UTF-8";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(temp), charset));
        String currentLine;
        while ((currentLine = reader.readLine()) != null) {
            currentLine = currentLine.trim();

            // Remove subtitle
            currentLine = currentLine.replaceAll("={2}.*={2}", "");

            // Remove non-ASCII characters
            currentLine = currentLine.replaceAll("[^A-Za-z0-9\\[\\]]"," ");

            // Remove stopwords
            Matcher titleMatcher = titlePattern.matcher(currentLine);
            List<String> allWords = new ArrayList<String>(Arrays.asList(currentLine.toLowerCase().split(" ")));
            if(!titleMatcher.find()){
                allWords.removeAll(stopWordList);
                currentLine = String.join(" ", allWords);
                currentLine = ' '+currentLine+' ';
                if(currentLine.equals("")){
                    continue;
                }
            }else{
                currentLine = String.join(" ", allWords);
                currentLine = '\n'+currentLine;
            }

            // Remove extra space.
            currentLine = currentLine.replaceAll(" +", " ");
            writer.print(currentLine);
        }
        writer.close();
        reader.close();
        return temp;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();


        textCleaner("/home/molin/Documents/Data/input/sample.txt");
        long end = System.currentTimeMillis();
        long timeElapsed = end - start;
        System.out.println("Elapsed time:"+timeElapsed/1000F);
        // String testTarFile =
        // "/home/molin/Documents/Data/input/20140615-wiki-en_000000.txt.gz";
        // textPreprocess(testTarFile);
    }

}
