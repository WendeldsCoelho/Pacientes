import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao10 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Adicionar coluna de faixa etária
    val dfFaixas = dfRenomeado
      .withColumn(
        "faixa_etaria",
        when(col("idade") <= 18, "0-18")
          .when(col("idade").between(19, 30), "19-30")
          .when(col("idade").between(31, 60), "31-60")
          .otherwise("60+")
      )

    // Contar pacientes por faixa etária
    val faixas = dfFaixas
      .groupBy("faixa_etaria")
      .count()
      .orderBy("faixa_etaria")
      .collect()

    // Extrair faixas etárias e quantidades
    val faixasEtarias = faixas.map(_.getString(0)).toSeq // Converter para Seq[String]
    val quantidades = faixas.map(_.getLong(1)).toSeq     // Converter para Seq[Long]

    // Criar o gráfico de barras
    val grafico = Bar(
      x = faixasEtarias, // Faixas etárias no eixo X
      y = quantidades    // Quantidades no eixo Y
    )
      .withText(quantidades.map(_.toString)) // Adiciona rótulos com valores
      .withTextposition(BarTextPosition.Outside) // Mostra os valores fora das barras

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Distribuição de Pacientes por Faixa Etária")
      .withXaxis(
        Axis()
          .withTitle("Faixa Etária")
      )
      .withYaxis(
        Axis()
          .withTitle("Número de Pacientes")
      )

    // Gerar e salvar o gráfico
    val caminhoArquivo = "distribuicao_faixas_bar.html"
    Plotly.plot(
      path = caminhoArquivo,           // Caminho do arquivo de saída
      traces = Seq(grafico),           // Gráfico de barras
      layout = layout,                 // Layout configurado
      config = Config(),               // Configuração padrão
      useCdn = true,                   // Usar CDN para carregar bibliotecas
      openInBrowser = true,            // Abre automaticamente no navegador
      addSuffixIfExists = true         // Adiciona sufixo se o arquivo já existir
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}