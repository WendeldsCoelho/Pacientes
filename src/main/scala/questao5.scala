import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao5 {
  def run(dfRenomeado: DataFrame): Unit = {
    val dataAtual = current_date()
    dfRenomeado
      .withColumn("data_fim_estimada", expr("date_add(`Data de início`, `Duração do tratamento`)"))
      .filter(row => row.getAs[java.sql.Date]("data_fim_estimada").after(java.sql.Date.valueOf(dataAtual.toString)))
      .select("atendimento", "nomePaciente", "diagnostico", "Médico responsável")
      .show(false)
  }
}