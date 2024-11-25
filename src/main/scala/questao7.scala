import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao7 {
  def run(dfRenomeado: DataFrame): Unit = {
    dfRenomeado
      .filter(col("medico") === "Dr. Silva")  // Filtra apenas os tratamentos do Dr. Silva
      .agg(round(sum("CustoTratamento"), 2).as("custoTotal"))  // Calcula o custo total e arredonda para duas casas
      .select("custoTotal")  // Seleciona apenas a coluna do custo total
      .show(false)  // Exibe o valor sem truncamento

  }
}