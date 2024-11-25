import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao1 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Filtrar dados de pacientes acima de 60 anos que receberam "Fisioterapia"
    val resultado = dfRenomeado
      .filter(col("idade") > 60 && col("tratamento") === "Fisioterapia")
      .select("atendimento", "nomePaciente", "diagnostico")
      .collect()

    // Converter resultados para Seq
    val atendimentos: Seq[String] = resultado.map(_.getInt(0).toString).toSeq // Eixo X
    val diagnosticos: Seq[String] = resultado.map(_.getString(2)).toSeq      // Eixo Y

    // Criar o gráfico de barras
    val grafico = Bar(
      x = atendimentos,   // Eixo X: IDs dos atendimentos
      y = diagnosticos    // Eixo Y: Diagnósticos
    )

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Diagnósticos por Atendimento")
      .withXaxis(
        Axis()
          .withTitle("Atendimentos")
          .withTickangle(-45)
          .withTickfont(Font().withSize(10))
      )
      .withYaxis(Axis().withTitle("Diagnósticos"))
      .withMargin(Margin(60, 30, 50, 100))

    // Gerar e salvar o gráfico
    val caminhoArquivo = "grafico_diagnosticos_por_atendimento.html"
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
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//
//object questao1 {
//  def run(dfRenomeado: DataFrame): Unit = {
//    // Filtrar dados de pacientes acima de 60 anos que receberam "Fisioterapia"
//    val resultado = dfRenomeado
//      .filter(col("idade") > 60 && col("tratamento") === "Fisioterapia")
//      .select("atendimento", "nomePaciente", "diagnostico")
//      .show(Int.MaxValue, truncate = false)
    //val todosOsResultados = resultado.collect()
    //todosOsResultados.foreach(println)

//    // Mostrar os dados formatados no console
//    println("Atendimentos de pacientes > 60 anos que receberam Fisioterapia:")
//    resultado.show(truncate = false)
//
//    // Caso precise salvar como CSV para análise posterior
//    val caminhoArquivo = "resultados_fisioterapia.csv"
//    resultado
//      .write
//      .option("header", "true")
//      .csv(caminhoArquivo)
//
//    println(s"Resultados salvos em: $caminhoArquivo")
//  }
//}