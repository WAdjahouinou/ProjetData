package fr.project.info;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for simple App.
 */
@Slf4j
public class MainTest {
/*    private static final Config testConf = ConfigFactory.load("application.conf");

    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp() {
        log.info("initializing sparkSession ...");
        sparkSession = SparkSession.builder()
                .master(testConf.getString("app.master"))
                .appName(testConf.getString("app.name"))
                .getOrCreate();
    }*/

    @Test
    public void shouldGenerateOutput() {
        log.info("running IT");
        String[] testArgs = new String[]{};
        Main.main(testArgs);
        assertThat(Files.exists(Paths.get(ConfigFactory.load("application.conf").
                getString("app.data.output.path")))).isTrue();
        //assertThat("app.data.output.path" ).isNotEmpty();
    }
}
