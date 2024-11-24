import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao4 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcula a média da idade
    val mediaIdade = dfRenomeado
      .agg(avg("idade").as("mediaIdade"))
      .collect()
      .head
      .getDouble(0)

    // Adiciona coluna de desvio absoluto para calcular o desvio médio
    val dfComDesvios = dfRenomeado.withColumn(
      "desvioAbsoluto",
      abs(col("idade") - lit(mediaIdade))
    )

    // Calcula desvio médio e desvio padrão
    val estatisticas = dfComDesvios
      .agg(
        round(avg("desvioAbsoluto"), 2).as("desvioMedio"),
        round(stddev("idade"), 2).as("desvioPadrao")
      )
      .collect()
      .head

    val desvioMedio = estatisticas.getDouble(0)
    val desvioPadrao = estatisticas.getDouble(1)

    // Exibe os resultados no console
    println(f"Média de idade: $mediaIdade%.2f")
    println(f"Desvio médio: $desvioMedio%.2f")
    println(f"Desvio padrão: $desvioPadrao%.2f")

    // Dados para o gráfico
    val grafico = Bar(
      x = Seq("Média", "Desvio Padrão", "Desvio Médio"),
      y = Seq(mediaIdade, desvioPadrao, desvioMedio)
    )

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Estatísticas da Idade dos Pacientes")
      .withXaxis(Axis().withTitle("Métricas"))
      .withYaxis(Axis().withTitle("Valor (anos)"))

    // Gera o gráfico em um arquivo HTML
    val caminhoArquivo = "grafico_estatisticas_idade.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      config = Config(),
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")
  }
}