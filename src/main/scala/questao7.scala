import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object questao7 {
  def run(dfRenomeado: DataFrame): Unit = {
    dfRenomeado
      .filter(col("Médico responsável") === "Dr. Silva")  // Filtra apenas os tratamentos do Dr. Silva
      .agg(round(sum("Custo do tratamento"), 2).as("custoTotal"))  // Calcula o custo total e arredonda para duas casas
      .select("custoTotal")  // Seleciona apenas a coluna do custo total
      .show(false)  // Exibe o valor sem truncamento
  }
}