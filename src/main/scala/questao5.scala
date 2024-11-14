import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// RELEMBRAR DE VERIFICAR SE O CÓDIGO ESTÁ CORRETAMENTE FUNCIONANDO
object questao5 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcula a data final estimada para cada tratamento
    val dfComDataFim = dfRenomeado.withColumn("data_fim_estimada", expr("date_add(dataInicio, duracao)"))

    // Filtra os registros onde a data final estimada é posterior à data atual
    dfComDataFim
      .filter(col("data_fim_estimada") > current_date())
      .select("atendimento", "nomePaciente", "diagnostico", "Médico responsável")
      .show(false)
  }
}