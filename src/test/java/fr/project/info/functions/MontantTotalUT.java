package fr.project.info.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MontantTotalUT {
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
    public void testMontantTotal() {
        log.info("running test on reader ...");
        String testInputPath = "target/test-classes/data/input/JeuDeDonnées.txt";
        Dataset<Row> ds =
                new DatasetTextFileReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        ;
        /*Dataset<Record> dataset = sparkSession.createDataset(
                Arrays.asList(new Record("commune1", "4"),
                        new Record("commune2", "6")), Encoders.bean(Record.class)
        );
        Dataset<Row> ds =  dataset.toDF();
        ds.printSchema();
        Dataset<Row> ds1 = ds.withColumnRenamed("libelleCommune", "LIBELLE DE LA COMMUNE DE RESIDENCE")
        .withColumnRenamed("montantTotal", "MONTANT TOTAL");
        ds1.printSchema();
        Dataset<Row> fil = new MontantTotal().apply(ds1);*/

        Dataset<Row> fil = new MontantTotal().apply(ds);
        /*Dataset<Row> join =  ds.select("CODE POSTAL DE LA COMMUNE DE RESIDENCE");
        Dataset<Row> result = fil.join(join);*/

        log.info("show sample with truncate=true");
        fil.show(false);
        long numberOfLines = fil.count();
        long numberOfLines2 = ds.count();
        log.info(" Nombres de lignes avant le filtre = {} , Nombre de ligne apres le filtre = {}", numberOfLines2, numberOfLines);
        //assertThat(numberOfLines).isEqualTo(2);
        assertThat(numberOfLines2).isGreaterThan(numberOfLines);
    }

    @Test
    public void testMontantTotal2() {
        /*log.info("running test on reader ...");
        String testInputPath = "target/test-classes/data/input/JeuDeDonnées.txt";
        Dataset<Row> ds =
                new DatasetTextFileReader(
                        sparkSession, testInputPath
                )
                        .get()
                        .cache();
        ;*/
        Dataset<Record> dataset = sparkSession.createDataset(
                Arrays.asList(new Record("commune1", "4"),
                        new Record("commune1", "6")), Encoders.bean(Record.class)
        );
        Dataset<Row> ds =  dataset.toDF();
        ds.printSchema();
        Dataset<Row> ds1 = ds.withColumnRenamed("libelleCommune", "LIBELLE DE LA COMMUNE DE RESIDENCE")
                .withColumnRenamed("montantTotal", "MONTANT TOTAL");
        ds1.printSchema();
        Dataset<Row> fil = new MontantTotal().apply(ds);
        log.info("show sample with truncate=true");
        fil.show(false);
        fil.printSchema();
        long numberOfLines = fil.count();
        long numberOfLines2 = ds.count();
        log.info(" Nombres de lignes avant le filtre = {} , Nombre de ligne apres le filtre = {}", numberOfLines2, numberOfLines);
        /*assertThat(numberOfLines).isEqualTo(1);
        Long montant_total_des_aides = fil.head().getAs("MONTANT TOTAL DES AIDES");
        assertThat(montant_total_des_aides).isEqualTo(10);*/
        assertThat(numberOfLines2).isGreaterThan(numberOfLines);
    }

}
