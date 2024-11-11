import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao4 {
  def run(dfRenomeado: DataFrame) : Unit = {
      dfRenomeado
        .agg(round(avg("idade") ,2).as("mediaIdade"))
        .select("mediaIdade")
        .show(false)
  }
}