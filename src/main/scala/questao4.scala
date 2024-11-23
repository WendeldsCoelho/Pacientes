import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao4 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Calcula a média de idade
    val mediaIdade = dfRenomeado
      .agg(round(avg("idade"), 2).as("mediaIdade"))
      .collect()(0).getDouble(0)

    // Dados do gráfico (exemplo para uma linha simples com apenas um ponto)
    val xValues = Seq("Média de Idade") // Eixo X com uma categoria
    val yValues = Seq(mediaIdade)  // Média calculada no eixo Y

    // Cria o gráfico de linha
    val grafico = Scatter()
      .withX(xValues)
      .withY(yValues)
      //.withMode(ScatterMode.Lines) // Apenas linhas
      //.withLine(Line().withColor("blue").withWidth(2))   // Configuração da linha
      .withName("Média de Idade")

    // Layout personalizado
    val layout = Layout()
      .withTitle("Média de Idade dos Pacientes")
      .withXaxis(Axis().withTitle("Categoria"))
      .withYaxis(Axis().withTitle("Idade Média"))
      .withPlot_bgcolor(Color.RGBA(245, 245, 245, 1.0)) // Cor de fundo cinza claro

    // Gera o gráfico em um arquivo HTML
    Plotly.plot(
      path = "media_idade_linha.html",
      traces = Seq(grafico),
      layout = layout
    )
  }
}