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

    public void readStopWordFile(String stopWordFile_PATH){
        try{
            BufferedReader fis = new BufferedReader(new FileReader(stopWordFile_PATH));
        }catch(IOException ioe){
            System.err.println("Exception while reading stop word file"
            +stopWordFile_PATH+" "+ioe.toString());
        }
    }
    public static void textPreprocess(String filePath){
        try{
            File tarFile = new File(filePath);
            String par_dir =tarFile.getParent();
            System.out.println(tarFile.getParent());
            //File outFile = new File()
            //extractGZip(tarFile, )
        }catch(Exception ioe){
            System.err.println("Exception while reading tar file"
                    +filePath+" "+ioe.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        String testTarFile = "/home/molin/Documents/Data/input/20140615-wiki-en_000000.txt.gz";
        textPreprocess(testTarFile);
    }

}
