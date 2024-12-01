import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao3 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Consulta original: Agrupar por tratamento e contar a quantidade de pacientes com duração > 30 dias
//    val resultadoTerminal = dfRenomeado
//      .filter(col("duracao") > 30)
//      .select("atendimento", "tratamento", "duracao", "medico")
//      .show(Int.MaxValue, truncate = false)
//    Nova consulta: Quantidade de Pacientes por Tratamento (> 30 dias)
    val resultadoColetado = dfRenomeado
      .filter(col("duracao") > 30)
      .groupBy("tratamento")
      .agg(count("tratamento").alias("quantidade_pacientes"))
      .collect()
      .map(row => (row.getAs[String]("tratamento"), row.getAs[Long]("quantidade_pacientes")))
    // Converter para sequências
    val tratamentos = resultadoColetado.map(_._1).toSeq
    val quantidades = resultadoColetado.map(_._2.toDouble).toSeq

    // Criar o gráfico de barras
    val trace = Bar(
      x = tratamentos,
      y = quantidades
    ).withName("Pacientes por Tratamento")
      .withMarker(Marker().withColor(Color.RGBA(0, 100, 0, 0.7)))


    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Quantidade de Pacientes por Tratamento (> 30 Dias)")
      .withXaxis(Axis().withTitle("Tratamentos").withTickangle(45).withAutomargin(true))
      .withYaxis(Axis().withTitle("Quantidade de Pacientes"))
      .withHeight(800)
      .withMargin(Margin(60, 30, 100, 100))
      .withShowlegend(false)

    // Plotar e salvar o gráfico
    val caminhoArquivo = "Q3_grafico_pacientes_por_tratamento.html"
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