package fr.project.info.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.function.Function;

public class MontantTotal implements Function<Dataset<Row>, Dataset<Row>> {

    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset) {
        Dataset<Row> filtre = rowDataset.select("MONTANT TOTAL", "LIBELLE DE LA COMMUNE DE RESIDENCE","CODE POSTAL DE LA COMMUNE DE RESIDENCE")
                .withColumn("MONTANT TOTAL", rowDataset.col("MONTANT TOTAL").cast("int"))
                .groupBy("LIBELLE DE LA COMMUNE DE RESIDENCE")
                .agg(functions.sum("MONTANT TOTAL").as("MONTANT TOTAL DES AIDES"),
                        functions.first("CODE POSTAL DE LA COMMUNE DE RESIDENCE").as("CODE POSTAL DE LA COMMUNE DE RESIDENCE"),
                        functions.collect_set("CODE POSTAL DE LA COMMUNE DE RESIDENCE").as("CODE POSTAUX")
                        );

/*join(rowDataset.select("CODE POSTAL DE LA COMMUNE DE RESIDENCE"),
                                rowDataset.col("CODE POSTAL DE LA COMMUNE DE RESIDENCE")
                        .equalTo())*/

        return filtre
                .orderBy(functions.col("MONTANT TOTAL DES AIDES").desc_nulls_last());
    }
}
