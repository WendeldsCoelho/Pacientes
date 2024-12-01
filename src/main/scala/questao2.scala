import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly
import plotly.element.BarTextPosition.Outside
import plotly.element.Error.Data

object questao2{
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcular a média do custo do tratamento
    val mediaCusto = dfRenomeado
      .agg(avg("custoTratamento").alias("mediaCusto"))
      .collect()
      .head
      .getDouble(0)

    // Adicionar a coluna de diferença absoluta em relação à média para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("custoTratamento") - lit(mediaCusto))
    )

    // Cálculo do desvio médio
    val desvioMedio = dfComDesvios
      .agg(round(avg(col("desvioAbsoluto")), 2).as("desvioMedio"))
      .collect()
      .head
      .getDouble(0)

    // Criação do gráfico
    val traceMedia = Bar(
      x = Seq("custoTratamento"),
      y = Seq(mediaCusto)
    ).withName("Média do Custo")
      .withError_y(
        Data(
          array = Seq(desvioMedio),
          visible = true
        )
      )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Média do Custo do Tratamento com Desvio Médio")
      .withXaxis(Axis().withTitle("Tratamento"))
      .withYaxis(Axis().withTitle("Valor (R$)"))
      .withMargin(Margin(60, 60, 50, 60))
      .withWidth(600)
      .withHeight(500)

    // Salvamento do gráfico
    val caminhoArquivo = "grafico_media_custo_com_desvio.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(traceMedia),
      layout = layout,
      config = Config(),
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

  }
}