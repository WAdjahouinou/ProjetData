package fr.project.info.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.project.info.functions.DatasetTextFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DatasetTextFileReaderUT {

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

    @Before
    public void beforeEachTest(){
        log.info("we are about  to  run a  new test ...");
    }

    @Test
    public void testReaderEmptyPath(){
        log.info("running test on reader ...");
        String testInputPath = "target/test-classes/fake-path";
        DatasetTextFileReader reader = new DatasetTextFileReader(
                sparkSession, testInputPath
        );
        Dataset<Row> ds = reader.get();
        ds.show(6, false);
        ds.printSchema();
        assertThat(ds.count()).isEqualTo(0);
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
        log.info("show sample with truncate=true");
        ds.show(6);
        log.info("show sample with truncate=false");
        ds.show(6, false);
        log.info("print schema");
        ds.printSchema();
        long nbLines = ds.count();
        log.info("nbLines={}", nbLines);
        assertThat(nbLines).isGreaterThan(0);
        assertThat(ds.javaRDD().isEmpty()).isFalse();

        ds.unpersist();
    }


    @After
    public void afterEachTest(){
        log.info("we have just completed a test!");
    }

    @AfterClass
    public static void tearDown(){
        log.info("all tests completed!");
    }
}
