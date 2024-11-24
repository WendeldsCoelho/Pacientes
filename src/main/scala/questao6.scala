import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao6 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Filtrar os atendimentos com diagnóstico de "Hipertensão"
    val atendimentosHipertensao = dfRenomeado
      .filter(col("diagnostico") === "Hipertensão")
      .select("nomePaciente", "tratamento", "Custo do tratamento")
      .collect()

    // Preparar os dados para o gráfico
    val pacientes = atendimentosHipertensao.map(_.getString(0)).toSeq        // Convertido para Seq
    val tratamentos = atendimentosHipertensao.map(_.getString(1)).toSeq     // Convertido para Seq
    val custos = atendimentosHipertensao.map(_.getDouble(2)).toSeq          // Convertido para Seq

    // Criar o gráfico de barras
    val grafico = Bar(
      x = pacientes,        // Eixo X: Nomes dos pacientes
      y = custos,           // Eixo Y: Custos dos tratamentos
    ).withName("Custo do Tratamento")
      .withText(tratamentos) // Mostrar o tratamento no rótulo do gráfico

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Custos dos Tratamentos para Hipertensão")
      .withXaxis(
        Axis()
          .withTitle("Pacientes")
          .withTickangle(-45) // Inclinar os rótulos do eixo X
      )
      .withYaxis(Axis().withTitle("Custo do Tratamento (R$)"))
      .withMargin(Margin(80, 50, 100, 100)) // Ajuste de margens

    // Gerar o gráfico em um arquivo HTML
    val caminhoArquivo = "grafico_hipertensao.html"
    Plotly.plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      useCdn = true,
      openInBrowser = true,
      addSuffixIfExists = true
    )

    println(s"Gráfico gerado e aberto no navegador: $caminhoArquivo")
  }
}