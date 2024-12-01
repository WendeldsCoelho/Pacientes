import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly
import plotly.element.Error.Data

object questao9 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Custo médio por diágostico com desvio padrão
    val custosPorDiagnostico = dfRenomeado
      .groupBy("diagnostico")
      .agg(
        avg(col("custoTratamento")).alias("custo_medio"),
        stddev(col("custoTratamento")).alias("desvio_padrao")
      )
      .orderBy(col("custo_medio").desc)

    // Dados para o gráfico
    val diagnosticosColeta = custosPorDiagnostico.collect().map(row =>
      (
        row.getAs[String]("diagnostico"),
        row.getAs[Double]("custo_medio"),
        row.getAs[Double]("desvio_padrao")
      )
    )

    // Extração de dados para o gráfico
    val diagnosticos = diagnosticosColeta.map(row => row._1)
    val custoMedio = diagnosticosColeta.map(row => row._2)
    val desvio = diagnosticosColeta.map(row => row._3)

    // Criação do gráfico
    val trace = Bar(
      x = diagnosticos.toSeq,
      y = custoMedio.toSeq
    ).withName("Customédio")
      .withError_y(
        Data(
          array = desvio
        )
          .withVisible(true)
      )
      .withMarker(
        Marker().withColor(Color.RGBA(100, 149, 237, 0.6))
      )

    val layout = Layout()
      .withTitle("Custo Médio por Diagnóstico com Desvio Padrão")
      .withXaxis(Axis()
        .withTitle("Diagnóstico"))
      .withYaxis(Axis()
        .withTitle("Custo Médio (R$)"))
      .withShowlegend(false)

    // Salvamento do gráfico
    val caminhoArquivo = "grafico_custo_medio_por_diagnostico.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(trace),
      layout = layout,
      config = Config(),
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

    println(s"Gráfico salvo e aberto no navegador: $caminhoArquivo")

    // Exibição dos resultados
    custosPorDiagnostico.show(false)
  }
}
