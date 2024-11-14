import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao10 {
  def run(dfRenomeado: DataFrame): Unit = {
    dfRenomeado
      .withColumn("faixa_etaria",
        when(col("idade") <= 18, "0-18")
          .when(col("idade").between(19, 30), "19-30")
          .when(col("idade").between(31, 60), "31-60")
          .otherwise("60+"))
      .groupBy("faixa_etaria")
      .count()
      .orderBy("faixa_etaria")
      .show(false)
  }
}