import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// RELEMBRAR DE VERIFICAR SE O CÓDIGO ESTÁ CORRETAMENTE FUNCIONANDO
object questao5 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Adiciona coluna calculada com a data final estimada do tratamento
    val dfComDataFim = dfRenomeado.withColumn(
      "data_fim_estimada",
      expr("date_add(dataInicio, duracao)")
    )

    // Filtra os tratamentos em andamento (data_fim_estimada > data atual)
    val tratamentosAtivos = dfComDataFim
      .filter(col("data_fim_estimada") > current_date())

    // Seleciona as colunas solicitadas
    tratamentosAtivos
      .select("atendimento", "nomePaciente", "diagnostico", "medico")
      .show(false) // Exibe os resultados no console
  }
}