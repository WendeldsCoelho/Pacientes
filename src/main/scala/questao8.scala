import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao8 {
  def run(dfRenomeado: DataFrame): Unit = {
    dfRenomeado
      .agg(round(avg("duracao"), 2).as("duracaoMedia"))  // Calcula a média e arredonda
      .select("duracaoMedia")  // Seleciona apenas a coluna de duração média
      .show(false)  // Exibe o valor sem truncar
  }
}