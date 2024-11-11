import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao2 {
  def run(dfRenomeado: DataFrame) : Unit = {
    val mediaCusto = dfRenomeado
      .select(round(avg(col("Custo do tratamento")), 2)
        .as("mediaCustoTratamento"))

    mediaCusto.show(false)
  }
}