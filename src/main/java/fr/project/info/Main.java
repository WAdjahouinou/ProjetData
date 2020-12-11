package fr.project.info;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.project.info.functions.DatasetCsvWritter;
import fr.project.info.functions.DatasetTextFileReader;
import fr.project.info.functions.MontantTotal;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
@Slf4j
public class Main
{
    public static void main( String[] args ) {
        log.info( "Starting Spark Application" );

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");
        String inputPath = config.getString("app.data.input.path");
        String outputPath = config.getString("app.data.output.path");


        SparkConf sparkConf = new SparkConf()
                .setMaster(masterUrl).setAppName(appName)
                .setJars(new String[]{config.getString("app.data.jar.path")})
                .set("spark.executor.instances", String.valueOf(config.getInt("app.executor.nb")))
                .set("spark.executor.memory", config.getString("app.executor.memory"))
                .set("spark.executor.cores", config.getString("app.executor.cores"));

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

       /* DatasetTextFileReader reader = new DatasetTextFileReader(sparkSession, inputPath);
        MontantTotal function = new MontantTotal();
        DatasetCsvWritter<Row> writer = new DatasetCsvWritter<>(outputPath);

        Dataset<Row> raw = reader.get();
        Dataset<Row> result = function.apply(raw);
        writer.accept(result);

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();*/

        Dataset<Row> reader = new DatasetTextFileReader(sparkSession, inputPath).get();

        Dataset<Row> fil = new MontantTotal().apply(reader);
        DatasetCsvWritter write = new DatasetCsvWritter(outputPath + "_txt" );
        write.accept(fil);

        boolean enableSleep = config.getBoolean("app.data.sleep");
        log.info("isSleeping={} for 5min", enableSleep);
        if(enableSleep){
            try {
                Thread.sleep(1000 * 60 * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("Done");
    }
}
