import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao7 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Consulta original: calcular o custo total do "Dr. Silva"
    dfRenomeado
      .filter(col("medico") === "Dr. Silva")
      .agg(round(sum("CustoTratamento"), 2).as("custoTotal"))
      .select("custoTotal")
      .show(false)

    // Nova consulta: comparar custo total entre todos os médicos
    val resultadoColetado = dfRenomeado
      .groupBy("medico")
      .agg(round(sum("CustoTratamento"), 2).as("custoTotal"))
      .collect()
      .map(row => (row.getAs[String]("medico"), row.getAs[Double]("custoTotal")))

    // Converção para sequências para o gráfico
    val medicos = resultadoColetado.map(_._1).toSeq
    val custosTotais = resultadoColetado.map(_._2).toSeq

    // Criação do gráfico
    val trace = Bar(
      x = medicos,
      y = custosTotais
    ).withName("Custo Total por Médico")
      .withMarker(Marker().withColor(Color.RGBA(255, 69, 0, 0.7)))

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Comparação de Custo Total por Médico")
      .withXaxis(Axis()
        .withTitle("Médicos")
        .withTickangle(-45)
      )
      .withYaxis(Axis().withTitle("Custo Total (R$)"))
      .withMargin(Margin(60, 30, 50, 100))
      .withShowlegend(false)

    // Salvamento do gráfico
    val caminhoArquivo = "grafico_custo_total_por_medico.html"
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
  }
}