import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao9 {
  def run(dfRenomeado: DataFrame): Unit = {
    dfRenomeado
      .groupBy("diagnostico")
      .agg(round(avg("Custo do tratamento"), 2).as("custoMedio"))
      .orderBy(col("custoMedio").desc)
      .show(false)
  }
}