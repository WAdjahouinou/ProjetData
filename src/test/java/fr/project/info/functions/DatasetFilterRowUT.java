package fr.project.info.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DatasetFilterRowUT {
    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSession;
    @BeforeClass
    public static void setUp(){
        log.info("initializing sparkSession ...");
        sparkSession = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
    }

    @Test
    public void testReader(){
        log.info("running test on reader ...");
        String testInputPath = "target/test-classes/data/input/JeuDeDonnées.txt";
        Dataset<Row> ds =
                new DatasetTextFileReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        ;
        Dataset<Row> fil = new MontantTotal().apply(ds);
        log.info("show sample with truncate=true");
        fil.show(false);
        long nbLines = fil.count();
        long nblines2 = ds.count();
        log.info(" nbline avant filtre = {} , nbline apres filtre = {}", nblines2, nbLines );
        assertThat(nblines2).isGreaterThan(nbLines);
     /*
        Dataset<Row> dr = new DatasetFilterRow().apply(ds);
        log.info("show sample with truncate=true");
        dr.show(6);
        log.info("show sample with truncate=false");
        dr.show(6, false);
        log.info("print schema");
        dr.printSchema();
        long nbLines = dr.count();
        long nblines2 = ds.count();
        log.info(" nbline avant filtre = {} , nbline apres filtre = {}", nblines2, nbLines );
        assertThat(nblines2).isGreaterThan(nbLines);
        assertThat(ds.javaRDD().isEmpty()).isFalse();

        ds.unpersist();*/
    }
}
