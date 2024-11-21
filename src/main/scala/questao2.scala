import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao2 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcular a média do custo do tratamento
    val mediaCusto = dfRenomeado
      .select(round(avg(col("Custo do tratamento")), 2)
        .as("mediaCustoTratamento"))
      .collect()

    // Extrair o valor da média do custo
    val mediaValor = mediaCusto.head.getDouble(0) // O valor da média

    // Criar o gráfico de barras com o valor da média
    val grafico = Bar(
      x = Seq("Média do Custo"),   // O eixo X terá o título "Média do Custo"
      y = Seq(mediaValor)          // O eixo Y terá o valor da média calculada
    )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Média do Custo do Tratamento")
      .withXaxis(
        Axis()
          .withTitle("Custo do Tratamento")
      )
      .withYaxis(
        Axis()
          .withTitle("Valor (R$)")
      )
      .withMargin(
        Margin(60, 30, 50, 100) // Definir as margens
      )

    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_media_custo.html"
    Plotly.plot(
      path = caminhoArquivo,           // Caminho do arquivo de saída
      traces = Seq(grafico),           // Gráfico com o valor da média
      layout = layout,                 // Layout configurado
      config = Config(),               // Configuração padrão
      useCdn = true,                   // Usar CDN para carregar bibliotecas
      openInBrowser = true,            // Abre automaticamente no navegador
      addSuffixIfExists = true         // Adiciona sufixo se o arquivo já existir
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}