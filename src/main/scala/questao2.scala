import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao2 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcular a média do custo
    val mediaCusto = dfRenomeado
      .select(avg(col("Custo do tratamento")).as("mediaCusto"))
      .collect()
      .head
      .getDouble(0)

    // Adicionar a coluna da diferença absoluta em relação à média para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("Custo do tratamento") - lit(mediaCusto))
    )

    // Calcular o desvio médio e o desvio padrão
    val estatisticas = dfComDesvios
      .agg(
        round(avg(col("desvioAbsoluto")), 2).as("desvioMedio"),
        round(stddev(col("Custo do tratamento")), 2).as("desvioPadrao")
      )
      .collect()
      .head

    val desvioMedio = estatisticas.getDouble(0)
    val desvioPadrao = estatisticas.getDouble(1)

    // Criar o gráfico de barras com os três valores
    val grafico = Bar(
      x = Seq("Média", "Desvio Padrão", "Desvio Médio"), // Rótulos do eixo X
      y = Seq(mediaCusto, desvioPadrao, desvioMedio)     // Valores correspondentes
    )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Estatísticas do Custo do Tratamento")
      .withXaxis(
        Axis()
          .withTitle("Métricas")
      )
      .withYaxis(
        Axis()
          .withTitle("Valor (R$)")
      )
      .withMargin(
        Margin(60, 60, 50, 60) // Ajusta as margens
      )
      .withWidth(600)
      .withHeight(500)


    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_estatisticas_custo.html"
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