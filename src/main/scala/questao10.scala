import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao10 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Adição da coluna de faixa etária
    val dfFaixas = dfRenomeado
      .withColumn(
        "faixa_etaria",
        when(col("idade") <= 18, "0-18")
          .when(col("idade").between(19, 30), "19-30")
          .when(col("idade").between(31, 60), "31-60")
          .otherwise("60+")
      )

    // Contagem de pacientes por faixa etária
    val faixas = dfFaixas
      .groupBy("faixa_etaria")
      .count()
      .orderBy("faixa_etaria")
      .collect()

    // Extraição de faixas etárias e quantidades
    val faixasEtarias = faixas.map(_.getString(0)).toSeq
    val quantidades = faixas.map(_.getLong(1)).toSeq

    // Criação do gráfico
    val grafico = Bar(
      x = faixasEtarias,
      y = quantidades
    )
      .withText(quantidades.map(_.toString))
      .withTextposition(BarTextPosition.Outside)
      .withMarker(Marker().withColor(Color.RGBA(255, 165, 0, 0.7)))

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
      .withMargin(
        Margin(100, 50, 50, 50) 
      )



    // Salvamento do gráfico
    val caminhoArquivo = "Q10_distribuicao_faixas_bar.html"
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