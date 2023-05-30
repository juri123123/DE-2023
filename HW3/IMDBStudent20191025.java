import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.*;

public final class IMDBStudent20191025 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("IMDBStudent20191025")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
            	ArrayList<String> genres = new ArrayList<String>();
            	String line[] = s.split("::");
            	String genre_list = line[2];
            	
            	StringTokenizer itr = new StringTokenizer(genre_list, "|");
            	while(itr.hasMoreTokens())
            	{
            		genres.add(itr.nextToken());
            	}
                return genres.iterator();
            }
        };
        
        JavaRDD<String> movie = lines.flatMap(fmf);

        PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
            	
                return new Tuple2(s, 1);
            }
        };
        
        JavaPairRDD<String, Integer> ones = movie.mapToPair(pf);

        Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        };
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
