import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object questao5 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Obter a data atual como String no formato "yyyy-MM-dd"
    val dataReferencia = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    // Adiciona coluna calculada com a data final estimada do tratamento
    val dfComDataFim = dfRenomeado.withColumn(
      "data_fim_estimada",
      expr("date_add(dataInicio, duracao)")
    )

    // Filtragem dos tratamentos em andamento com base na data de referência
    val tratamentosAtivos = dfComDataFim
      .filter(col("data_fim_estimada") > to_date(lit(dataReferencia), "yyyy-MM-dd"))

    // Agrupa por médico e conta a quantidade de casos ativos
    val casosPorMedico = tratamentosAtivos
      .groupBy("medico")
      .agg(count("atendimento").alias("quantidade_casos"))
      .orderBy(desc("quantidade_casos"))

    // Dados para o gráfico
    val dadosGrafico = casosPorMedico.collect().map(row =>
      (row.getString(0), row.getLong(1))
    )

    val medicos = dadosGrafico.map(_._1).toSeq
    val quantidades = dadosGrafico.map(_._2.toDouble).toSeq

    // Criação do gráfico
    val trace = Bar(
      x = medicos,
      y = quantidades
    ).withName("Casos Ativos por Médico")
      .withMarker(Marker().withColor(Color.RGBA(0, 0, 139, 0.7)))

    // Layout do gráfico
    val layout = Layout()
      .withTitle(s"Casos Ativos por Médico (Após $dataReferencia)")
      .withXaxis(Axis().withTitle("Médico").withTickangle(45))
      .withYaxis(Axis().withTitle("Quantidade de Casos"))
      .withMargin(Margin(60, 30, 50, 100))
      .withShowlegend(false)

    // Salvamento do gráfico em arquivo HTML
    val caminhoArquivo = "Q5_casos_ativos_por_medico.html"
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