package MapReduce;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static MapReduce.FileHandler.extractGZip;

/**
 * Preprocess the text file before feed into MapReduce.
 * TODO: Multi-thread unzip files.
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
     * @param stopWordFile_PATH the stop word file path
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
     *
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
     * Unzip.
     *
     * @param gzip_path   the gzip path
     * @param output_path the output path
     */
    public static void unzip(String gzip_path, String output_path){
        byte[] buffer = new byte[1024];
        try {
            FileInputStream fileIn = new FileInputStream(gzip_path);
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);

            FileOutputStream fileOutputStream = new FileOutputStream(output_path);
            int bytes_read;
            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytes_read);
            }
            gZIPInputStream.close();
            fileOutputStream.close();
            System.out.println("The file was decompressed successfully!");
        }catch (IOException ex){
            ex.printStackTrace();
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
            // If this line is header
            if(!titleMatcher.find()){
                allWords.removeAll(stopWordList);
                currentLine = String.join(" ", allWords);
                currentLine = ' '+currentLine+' ';
                currentLine = currentLine.replaceAll("\\]\\]", "");
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
     * Test unzip.
     *
     * @throws Exception the exception
     */
    @Test
    public void testUnzip() throws Exception{
        String test_path = "src/main/resources/Mockdata/20140615-wiki-en_000000.txt.gz";
        unzip(test_path, "src/main/resources/Mockdata/test.txt");
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        String testfile_PATH = "src/main/resources/sample.txt";
        textCleaner(testfile_PATH);
        long end = System.currentTimeMillis();
        long timeElapsed = end - start;
        System.out.println("Elapsed time:"+timeElapsed/1000F);
    }

}
