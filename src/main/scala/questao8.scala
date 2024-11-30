import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly
import plotly.element.Error.Data

object questao8 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcular a média da duração
    val mediaDuracao = dfRenomeado
      .agg(avg(col("duracao")).as("mediaDuracao"))
      .collect()
      .head
      .getDouble(0)

    // Adicionar a coluna da diferença absoluta em relação à média para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("duracao") - lit(mediaDuracao))
    )

    // Calcular o desvio médio
    val desvioMedio = dfComDesvios
      .agg(round(avg(col("desvioAbsoluto")), 2).as("desvioMedio"))
      .collect()
      .head
      .getDouble(0)

    // Criar o gráfico de barras com erro representando o desvio médio
    val traceMedia = Bar(
      x = Seq("Duração dos Tratamentos"),  // Nome da categoria
      y = Seq(mediaDuracao)                // Valor da média
    ).withName("Média da Duração")
      .withError_y(
        Data(
          array = Seq(desvioMedio),        // Desvio médio como erro
          visible = true                   // Tornar o erro visível
        )
      )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Média da Duração dos Tratamentos com Desvio Médio")
      .withXaxis(
        Axis()
          .withTitle("Categoria")
      )
      .withYaxis(
        Axis()
          .withTitle("Duração (dias)")
      )
      .withMargin(
        Margin(60, 60, 50, 60)
      )
      .withWidth(600)
      .withHeight(500)

    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_media_duracao_com_desvio.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(traceMedia),           // Usar o gráfico com erro
      layout = layout,                    // Layout configurado
      config = Config(),                  // Configuração padrão
      useCdn = true,                      // Usar CDN para carregar bibliotecas
      openInBrowser = true,               // Abre automaticamente no navegador
      addSuffixIfExists = true            // Adiciona sufixo se o arquivo já existir
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}