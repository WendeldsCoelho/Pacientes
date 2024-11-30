import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly
import plotly.element.Error.Data

object questao4 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcula a média da idade
    val mediaIdade = dfRenomeado
      .agg(avg("idade").alias("mediaIdade"))
      .collect()
      .head
      .getDouble(0)

    // Adiciona coluna de desvio absoluto para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("idade") - lit(mediaIdade))
    )

    // Calcula o desvio médio
    val desvioMedio = dfComDesvios
      .agg(round(avg("desvioAbsoluto"), 2).as("desvioMedio"))
      .collect()
      .head
      .getDouble(0)

    // Cria o gráfico de barras com erro representando o desvio médio
    val traceMedia = Bar(
      x = Seq("Idade Média"),
      y = Seq(mediaIdade)
    ).withName("Média da Idade")
      .withError_y(
        Data(
          array = Seq(desvioMedio),  // Representando o desvio médio como erro
          visible = true
        )
      )
      .withMarker(
        Marker().withColor(Color.RGBA(70, 130, 180, 0.8)) // Azul translúcido
      )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Média de Idade dos Pacientes com Desvio Médio")
      .withXaxis(Axis().withTitle("Métrica"))
      .withYaxis(Axis().withTitle("Idade (anos)"))
      .withMargin(Margin(60, 60, 50, 60))
      .withWidth(600)
      .withHeight(500)

    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_media_idade_com_desvio.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(traceMedia),
      layout = layout,
      config = Config(),
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}

