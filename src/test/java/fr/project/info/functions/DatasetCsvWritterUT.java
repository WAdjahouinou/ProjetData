package fr.project.info.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DatasetCsvWritterUT {
    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp() {
        log.info("initializing sparkSession ...");
        sparkSession = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
    }

    @Test
    public void testMontantTotal(){
        log.info("running test on reader ...");
        String testInputPath = testConf.getString("app.data.input.path");
        String outputPath = testConf.getString("app.data.output.path");
        Dataset<Row> ds =
                new DatasetTextFileReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        Dataset<Row> fil = new MontantTotal().apply(ds);
        DatasetCsvWritter write = new DatasetCsvWritter(outputPath + "_txt" );
        write.accept(fil);

        assertThat(outputPath + "_txt").isNotEmpty();
    }
}
