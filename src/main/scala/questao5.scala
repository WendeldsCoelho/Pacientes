import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import plotly._
import plotly.element._
import plotly.layout._
import plotly.Plotly

object questao5 {
  def run(dfRenomeado: DataFrame): Unit = {
    // Adiciona coluna calculada com a data final estimada do tratamento
    val dfComDataFim = dfRenomeado.withColumn(
      "data_fim_estimada",
      expr("date_add(dataInicio, duracao)") // Calcula a data final estimada
    )

    // Define a data específica como referência
    val dataReferencia = "2024-02-25"

    // Filtra os tratamentos em andamento com base na data de referência
    val tratamentosAtivos = dfComDataFim
      .filter(col("data_fim_estimada") > to_date(lit(dataReferencia), "yyyy-MM-dd"))
    //.select("atendimento", "nomePaciente", "diagnostico","medico")

    // Agrupa por médico e conta a quantidade de casos ativos
    val casosPorMedico = tratamentosAtivos
      .groupBy("medico")
      .agg(count("atendimento").alias("quantidade_casos"))
      .orderBy(desc("quantidade_casos"))

    // Coleta os dados para o gráfico
    val dadosGrafico = casosPorMedico.collect().map(row =>
      (row.getString(0), row.getLong(1)) // Médico, Quantidade de casos
    )

    val medicos = dadosGrafico.map(_._1).toSeq          // Médicos
    val quantidades = dadosGrafico.map(_._2.toDouble).toSeq // Quantidade de casos

    // Criar o gráfico de barras
    val trace = Bar(
      x = medicos,
      y = quantidades
    ).withName("Casos Ativos por Médico")
      .withMarker(Marker().withColor(Color.RGBA(0, 0, 139, 0.7))) // Cor violeta translúcida

    // Layout do gráfico
    val layout = Layout()
      .withTitle("Casos Ativos por Médico (Após 2024-03-25)")
      .withXaxis(Axis().withTitle("Médico").withTickangle(45)) // Inclinação dos rótulos
      .withYaxis(Axis().withTitle("Quantidade de Casos"))
      .withMargin(Margin(60, 30, 50, 100))
      .withShowlegend(false)

    // Gera e salva o gráfico em arquivo HTML
    val caminhoArquivo = "casos_ativos_por_medico.html"
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