import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao1 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Nova consulta: Filtrar pacientes com idade > 60
     val dfFiltrado = dfRenomeado
      .filter(col("idade") > 60)
    // Consulta original: Filtrar pacientes com idade > 60 e tratamento de Fisioterapia
//    val dfFiltradoTerminal = dfRenomeado
//      .filter(col("idade") > 60 &&col("tratamento") === "Fisioterapia")
//      .select("atendimento", "nomePaciente", "diagnostico")
//      .show(false)

    // Agrupar por tratamento e contar a quantidade de pacientes
    val resultadoColetado = dfFiltrado
      .groupBy("tratamento")
      .agg(count("tratamento").alias("quantidade_pacientes"))
      .collect()
      .map(row => (row.getAs[String]("tratamento"), row.getAs[Long]("quantidade_pacientes")))

    // Converter para sequências
    val tratamentos = resultadoColetado.map(_._1).toSeq
    val quantidades = resultadoColetado.map(_._2.toDouble).toSeq

    // Criação do gráfico
    val trace = Bar(
      x = tratamentos,
      y = quantidades
    ).withName("Pacientes por Tratamento")
       .withMarker(Marker().withColor(Color.RGBA(0, 0, 139, 0.7)))


    // Layout do gráfico
    val layout = Layout()
      .withTitle("Quantidade de Pacientes (> 60 anos) por Tratamento")
      .withXaxis(Axis().withTitle("Tratamentos"))
      .withYaxis(Axis().withTitle("Quantidade de Pacientes"))
      .withMargin(Margin(60, 30, 50, 100))
      .withShowlegend(false)

    // Salvamento do gráfico
    val caminhoArquivo = "Q1_grafico_pacientes_por_tratamento_idade_maior_60.html"
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