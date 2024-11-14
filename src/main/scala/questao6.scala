import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao6 {
  def run(dfRenomeado: DataFrame): Unit = {
    dfRenomeado
      .filter(row => row.getAs[String]("diagnostico") == "Hipertensão")
      .select("atendimento", "nomePaciente", "tratamento", "Custo do tratamento")
      .show(false)
  }
}