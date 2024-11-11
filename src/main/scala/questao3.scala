import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao3 {
  def run(dfRenomeado: DataFrame) : Unit = {
      dfRenomeado.filter(row => row.getAs[Int]("duracao") > 30)
        .select("atendimento", "tratamento", "duracao", "Médico responsável")
        .show(false)
  }
}