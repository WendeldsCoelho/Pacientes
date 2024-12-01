import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao6 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Consulta original: Agrupar por diagnóstico e contar a quantidade de pacientes
//    val resultadoTerminal = dfRenomeado
//      .filter(col("diagnostico") === "Hipertensão")
//      .select("atendimento", "nomePaciente", "tratamento", "custoTratamento")
//      .show(Int.MaxValue, truncate = false)
//    Nova consulta: Quantidade de Pacientes por Diagnóstico
    val resultadoColetado = dfRenomeado
      .groupBy("diagnostico")
      .agg(count("diagnostico").alias("quantidade_pacientes"))
      .collect()
      .map(row => (row.getAs[String]("diagnostico"), row.getAs[Long]("quantidade_pacientes")))

    // Converção para sequências
    val diagnosticos = resultadoColetado.map(_._1).toSeq
    val quantidades = resultadoColetado.map(_._2.toDouble).toSeq

    // Criação do gráfico
    val trace = Bar(
      x = diagnosticos,
      y = quantidades
    ).withName("Pacientes por Diagnóstico")
      .withMarker(Marker().withColor(Color.RGBA(0, 0, 139, 0.7)))

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Quantidade de Pacientes por Diagnóstico")
      .withXaxis(Axis().withTitle("Diagnóstico"))
      .withYaxis(Axis().withTitle("Quantidade de Pacientes"))
      .withMargin(Margin(60, 30, 50, 100))
      .withShowlegend(false)

    // Salvamento do gráfico
    val caminhoArquivo = "grafico_pacientes_por_diagnostico.html"
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