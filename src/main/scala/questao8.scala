import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao8 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcular a média da duração
    val mediaDuracao = dfRenomeado
      .select(avg(col("duracao")).as("mediaDuracao"))
      .collect()
      .head
      .getDouble(0)

    // Adicionar a coluna da diferença absoluta em relação à média para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("duracao") - lit(mediaDuracao))
    )

    // Calcular o desvio médio e o desvio padrão
    val estatisticas = dfComDesvios
      .agg(
        round(avg(col("desvioAbsoluto")), 2).as("desvioMedio"),
        round(stddev(col("duracao")), 2).as("desvioPadrao")
      )
      .collect()
      .head

    val desvioMedio = estatisticas.getDouble(0)
    val desvioPadrao = estatisticas.getDouble(1)

    // Criar o gráfico de barras com os três valores
    val grafico = Bar(
      x = Seq("Média", "Desvio Padrão", "Desvio Médio"), // Rótulos do eixo X
      y = Seq(mediaDuracao, desvioPadrao, desvioMedio)   // Valores correspondentes
    )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Estatísticas da Duração dos Tratamentos")
      .withXaxis(
        Axis()
          .withTitle("Métricas")
      )
      .withYaxis(
        Axis()
          .withTitle("Dias")
      )
      .withMargin(
        Margin(60, 30, 50, 100) // Ajusta as margens
      )
      .withWidth(600)
      .withHeight(500)

    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_estatisticas_duracao.html"
    Plotly.plot(
      path = caminhoArquivo,           // Caminho do arquivo de saída
      traces = Seq(grafico),           // Gráfico com as três métricas
      layout = layout,                 // Layout configurado
      config = Config(),               // Configuração padrão
      useCdn = true,                   // Usar CDN para carregar bibliotecas
      openInBrowser = true,            // Abre automaticamente no navegador
      addSuffixIfExists = true         // Adiciona sufixo se o arquivo já existir
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}