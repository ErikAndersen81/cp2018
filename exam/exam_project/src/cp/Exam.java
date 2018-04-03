package cp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author Erik Andersen <erand17@student.sdu.dk>
 */
public class Exam
{
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final int BUFFER_SIZE = 8192;
    private static final ExecutorService WORKSTEALEXEC = Executors.newWorkStealingPool(CORES);
    private static final ExecutorService FIXEDTHREADEXEC = Executors.newFixedThreadPool(CORES);
    private static final List<Result> RESULT = new ArrayList<>();
    private static final CompletionService<Result> COMP = new ExecutorCompletionService<Result>(FIXEDTHREADEXEC);
    
    
    /**
     * Factory method for creating results. 
     * @param path The path to be returned by {@link Result.path }method.
     * @param number The value returned by Result.number() method.
     * @return an instance of Result.
     */
    private static Result newResult(Path path, int number ){
        return new Result() {
            @Override
            public Path path() {
                return path;
            }

            @Override
            public int number() {
                return number;
            }
        };
    }
    
    /**
	 * This method recursively visits a directory to find all the text
	 * files contained in it and its subdirectories.
	 * 
	 * You must consider only files ending with a ".txt" suffix.
	 * You are guaranteed that they will be text files.
	 * 
	 * You can assume that each text file contains a (non-empty)
	 * comma-separated sequence of
	 * numbers. For example: 100,200,34,25
	 * There won't be any new lines, spaces, etc., and the sequence never
	 * ends with a comma.
	 * You are guaranteed that each number will be at least or equal to
	 * 0 (zero), i.e., no negative numbers.
	 * 
	 * The search is recursive: if the directory contains subdirectories,
	 * these are also searched and so on so forth (until there are no more
	 * subdirectories).
	 * 
	 * This method returns a list of results.
	 * The list contains a result for each text file that you find.
	 * Each {@link Result} stores the path of its text file,
	 * and the lowest number (minimum) found inside of the text file.
	 * 
	 * @param dir the directory to search
	 * @return a list of results ({@link Result}), each giving the lowest number found in a file
	 */
    public static List< Result > m1( Path dir )
    {
        // call parseDir on the Directory and wait for the results.
        parseDir1(dir).forEach((f) -> {
            try {
                RESULT.add((Result) f.get());
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        // All threads are finished and we are ready to shut down.
        WORKSTEALEXEC.shutdownNow();
        try {
            WORKSTEALEXEC.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException ex) {
            Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
        }
        return RESULT;
    }
    
    /**
     * This method will parse a given path for subdirectories and .txt files
     * which then will be passed to {@link parseDir} or {@link parseFile} 
     * respectively. The calls are submitted to Exam.EXEC as new 
     * threads. 
     *  
     * @param path An instance of Path to be parsed.
     * @return A List of Future<Result>s containing the results of the calls 
     * made to parseFile and any potential recursive calls.
     * 
     */
    private static List<Future<Result>> parseDir1(Path path) {
        List<Future<Result>> partialResults = new ArrayList<>();
        List<Future<List<Future<Result>>>> partialResultsList = new ArrayList<>();
        
        // Parse the directory and create new threads for parsing either files or subdirectories.
        try (DirectoryStream<Path> stream =
                Files.newDirectoryStream(path)){
            stream.forEach(p -> {
                if (Files.isDirectory(p)){
                     partialResultsList.add(WORKSTEALEXEC.submit(() -> parseDir1(p)));
                }
                if (p.toString().endsWith(".txt")){
                    partialResults.add(WORKSTEALEXEC.submit(() -> parseFile1(p)));
                }
            });
        } catch (IOException ex) {
            Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Wait for every file and subdirectory parsed to return their results.
        partialResultsList.forEach((future) -> {
            try {
                List<Future<Result>> resultList = (List<Future<Result>>) future.get();
                resultList.forEach((r) -> partialResults.add(r));
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        return partialResults;
    }

    /**
     * Parses a text file containing comma separated integers and returns 
     * a {@link Result} with the given path and the minimum value found in 
     * the file.
     * 
     * @param path The path to the txt-file to be parsed.
     * @return {@link Result}
     * @throws IOException 
     */
    private static Result parseFile1(Path path) throws IOException {
        // Stream the file through a channel into a buffer.
        FileInputStream stream = new FileInputStream(path.toFile());
        int min;
        // open the stream in a try-with resource which automatically closes it when done.
        try (FileChannel channel = stream.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            boolean valIsSet = false; // is a value stored in val?
            boolean valIsNeg = false; // is the value negative?
            boolean minIsNeg = false; // is the minimum located value negative?
            min = Integer.MAX_VALUE;
            int val = 0;
            int bytesRead = channel.read(buffer);
            // Keep reading until there are no more bytes in the file
            while ( bytesRead != -1){
                // flip the buffer for reading.
                buffer.flip();
                // parse every byte in the buffer
                while (buffer.hasRemaining()){
                    byte c = buffer.get();
                    // does the byte represent a digit? (ASCII codes [48;57])
                    if ( 47 <  c  && c < 58 ){
                        // hooray we don't need to look further, since the lowest value is zero!
                        if ((c == 48) && (!valIsSet)){
                            return newResult(path,0);
                        }
                        val = val * 10 + (c-48);
                        valIsSet = true;
                    }
                    // if the ASCII chr "-" is read then the number is negative.
                    else if (c == 45){
                        valIsNeg = true;
                    }
                    // We have probably read comma or some other character
                    // that is not a digit or "-" and are ready to evaluate if
                    // the value is less than the current minimum.
                    else if (valIsSet){
                        valIsSet = false;
                        if ((minIsNeg && valIsNeg) || (!minIsNeg && !valIsNeg)){
                            min = val < min ? val : min;
                        }
                        else if (!minIsNeg && valIsNeg){
                            min = val;
                            minIsNeg = true;
                        }
                        if (valIsNeg && min > 0) min *= -1;
                        val = 0;
                        valIsNeg = false;
                    }
                }
                // Process the next block of the file.
                buffer.clear();
                bytesRead = channel.read(buffer);
            }
        }
        return newResult(path,min);
    }
    
	
	/**
	 * This method recursively visits a directory for text files with suffix
	 * ".dat" (notice that it is different than the one before)
	 * contained in it and its subdirectories.
	 * 
	 * You must consider only files ending with a .dat suffix.
	 * You are guaranteed that they will be text files.
	 * 
	 * Each .dat file contains some lines of text,
	 * separated by the newline character "\n".
	 * You can assume that each line contains a (non-empty)
	 * comma-separated sequence of
	 * numbers. For example: 100,200,34,25
	 * 
	 * This method looks for a .dat file that contains a line whose numbers,
	 * when added together (total), amount to at least (>=) parameter min.
	 * Once this is found, the method can return immediately
	 * (without waiting to analyse also the other files).
	 * The return value is a result that contains:
	 *	- path: the path to the text file that contains the line that respects the condition;
	 *  - number: the line number, starting from 1 (e.g., 1 if it is the first line, 3 if it is the third, etc.)
	 * 
	 */
	public static Result m2( Path dir, int min )
	{
            parseDir2(dir, min);
            Result res = checkResults(min);
            FIXEDTHREADEXEC.shutdownNow();
            System.out.println("res = " + res);
            return res;
        }
        
        private static Result checkResults (int min){
            Result res = null;
            while (true){
                try {
                    res = COMP.poll(min, TimeUnit.HOURS).get();
                    if (res.number()<= min) return res;
                } catch (InterruptedException ex) {
                    Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ExecutionException ex) {
                    Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
	private static void parseDir2(Path dir, int min) {
            try{
                DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
                stream.forEach(f -> {
                    if (f.toString().endsWith(".dat")){
                        FIXEDTHREADEXEC.execute(() -> parseFile2(f));
                    }
                    else if (Files.isDirectory(f)){
                        parseDir2(f, min);
                    }
                });
            } catch (IOException e) {
                Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, e);
            }
            
        }
        
        private static void parseFile2(Path f) {
            try {
                Scanner sc = new Scanner(f.toFile());
                int lineCount = 0;
                while(sc.hasNextLine()){
                    String nextLn = sc.nextLine();
                    int ln = ++lineCount;
                    try{
                        COMP.submit(() -> parseLine2(nextLn, ln, f));
                    } catch (RejectedExecutionException ex) {
                        // Executor is shut down which means we're done.
                    }
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Exam.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        
        private static Result parseLine2(String nextLine, int lineNum, Path f) {
            Integer val = 0;
            for(String s:nextLine.split(",")){
                val += Integer.parseInt(s);
            }
            return newResult(f,lineNum); // THIS IS WRONG!!!
        }
        
    
	/**
	 * Computes overall statistics about the occurrences of numbers in a directory.
	 * 
	 * This method recursively searches the directory for all numbers in all lines of .txt and .dat files and returns
	 * a {@link Stats} object containing the statistics of interest. See the
	 * documentation of {@link Stats}.
	 */
	public static Stats m3( Path dir )
	{
		throw new UnsupportedOperationException();
	}


    
    

}