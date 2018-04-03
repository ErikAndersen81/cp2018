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
         testm2();   
        }

    private static void testm2() {
            Path dir = Paths.get("/home/erik/NetBeansProjects/exam/cp2018/exam/data_example");
            long t1 =System.currentTimeMillis();
            Result res = Exam.m2(dir, 200);
            long t2 =System.currentTimeMillis();
            System.out.println(res.path() + "\t" + res.number());
            System.out.println("delta = " + (t2-t1));
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
