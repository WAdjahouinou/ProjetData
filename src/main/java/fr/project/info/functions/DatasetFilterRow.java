package fr.project.info.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

public class DatasetFilterRow implements Function <Dataset<Row>, Dataset<Row> > {

    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset) {
        return rowDataset.
                select("CODE POSTAL DE LA COMMUNE DE RESIDENCE","NOM PRENOM OU RAISON SOCIALE","LIBELLE DE LA COMMUNE DE RESIDENCE")
                .distinct();
    }
}
