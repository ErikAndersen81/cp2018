package cp;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import sun.management.FileSystem;



/**
 * This class is present only for helping you in testing your software.
 * It will be completely ignored in the evaluation.
 * 
 * @author Fabrizio Montesi <fmontesi@imada.sdu.dk>
 */
public class Main
{
	public static void main( String[] args )
	{
         testm3();   
        }

    private static void testm3() {
            Path dir = Paths.get("/home/erik/NetBeansProjects/exam/cp2018/exam/data_example");
            long t1 =System.currentTimeMillis();
            Stats res = Exam.m3(dir);
            long t2 =System.currentTimeMillis();
            System.out.println("least freq:" + res.leastFrequent());
            System.out.println("most freq:" + res.mostFrequent());
            System.out.println("Totals 0: " + res.byTotals().get(0));
            System.out.println("Totals last: " + res.byTotals().get(res.byTotals().size()-1));
            System.out.println("took " + (t2-t1) + "ms");
        }
        
    private static void testm2() {
            Path dir = Paths.get("/home/erik/NetBeansProjects/exam/cp2018/exam/data_example");
            long t1 =System.currentTimeMillis();
            Result res = Exam.m2(dir, 2000000);
            long t2 =System.currentTimeMillis();
            System.out.println(res.path() + "\t" + res.number());
            System.out.println("took " + (t2-t1) + "ms");
        }
        
        private void testm1(){
            Path dir = Paths.get("/home/erik/NetBeansProjects/exam/cp2018/exam/data_example");
            long t1 =System.currentTimeMillis();
            List<Result> m1 = Exam.m1(dir);
            long t2 =System.currentTimeMillis();
            m1.forEach( x -> {
                System.out.println(x.path() + "\t" + x.number());
            }
            );
            System.out.println("delta = " + (t2-t1));
        }
}
