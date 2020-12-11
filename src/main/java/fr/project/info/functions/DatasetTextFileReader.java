package fr.project.info.functions;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class DatasetTextFileReader implements Supplier<Dataset<Row>> {
    @NonNull
    private final SparkSession sparkSession;
    @NonNull
    private final String inputPathStr;

    @Override
    public Dataset<Row> get() {
        log.info("reading data from inputPathStr = {}", inputPathStr);
        boolean hasValidInput = inputPathStr != null &&
                !inputPathStr.isEmpty() &&
                !sparkSession.sparkContext().isStopped();

        try {
            if (hasValidInput) {
                //return sparkSession.read().textFile(inputPathStr);
                return sparkSession.read().option("delimiter", ";").option("header","true").csv(inputPathStr);
            }
        } catch (Exception exception){
            log.info("could not read data with due to ...", exception);
        }

        return sparkSession.emptyDataFrame();
    }
}
