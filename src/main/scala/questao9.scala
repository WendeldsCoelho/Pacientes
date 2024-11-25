import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao9 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Agrupar os diagnósticos e calcular o custo médio
    val custos = dfRenomeado
      .groupBy("diagnostico")
      .agg(round(avg("custoTratamento"), 2).as("custoMedio"))
      .orderBy(col("custoMedio").desc)
      .collect()

    // Extrair diagnósticos e valores para o gráfico
    val diagnosticos = custos.map(_.getString(0)).toSeq // Converter para Seq
    val valores = custos.map(_.getDouble(1)).toSeq      // Converter para Seq

    // Criar o gráfico de barras
    val grafico = Bar(
      x = diagnosticos,
      y = valores
    )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Comparação de Custos Médios por Diagnóstico")
      .withXaxis(Axis().withTitle("Diagnóstico"))
      .withYaxis(Axis().withTitle("Custo Médio (R$)"))

    // Gerar o gráfico
    Plotly.plot(
      path = "comparacao_custos.html",
      traces = Seq(grafico),
      layout = layout
    )

    println("Gráfico de comparação de custos salvo como comparacao_custos.html.")
  }
}